package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"sync"
)

func main() {
	port := flag.String("port", "8080", "server port")

	flag.Parse()

	httpFifo := newHttpFifo()

	server := http.Server{
		Addr:        fmt.Sprint(":", *port),
		IdleTimeout: 5 * time.Minute,
	}

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := strings.TrimLeft(r.URL.Path, "/")
		queryParam := r.URL.Query()

		switch r.Method {
		case http.MethodPut:
			if !queryParam.Has("v") {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			httpFifo.push(queueName, queryParam.Get("v"))
		case http.MethodGet:
			elem := httpFifo.get(queueName)
			w.Write([]byte(elem))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	log.Fatal(server.ListenAndServe())
}

type httpFifo struct {
	queue map[string][]*string
	mutex sync.Mutex
}

func newHttpFifo() *httpFifo {
	return &httpFifo{
		queue: make(map[string][]*string),
	}
}

func (h *httpFifo) get(queueName string) string {
	for {
		_, ok := h.queue[queueName]
		if ok {
			h.mutex.Lock()
			defer h.mutex.Unlock()

			firstElem := h.queue[queueName][0]
			h.queue[queueName] = h.queue[queueName][1:]
			return *firstElem
		}
	}
}

func (h *httpFifo) push(queueName string, val string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.queue[queueName]; !ok {
		h.queue[queueName] = []*string{&val}
		return
	}
	h.queue[queueName] = append(h.queue[queueName], &val)
}
