package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"strings"

	"sync"
)

var (
	errNotInitializedQueue = errors.New("not initialized queue")
	errEmptyQueue          = errors.New("empty queue")
)

func main() {
	port := flag.String("port", "8080", "server port")

	flag.Parse()

	httpFifo := newHttpFifo()
	mux := http.NewServeMux()

	initQueueHandler(httpFifo, mux)

	log.Fatal(http.ListenAndServe(":"+*port, mux))
}

func initQueueHandler(fifo *httpFifo, mux *http.ServeMux) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		queueName := strings.TrimLeft(r.URL.Path, "/")
		queryParam := r.URL.Query()

		switch r.Method {
		case http.MethodPut:
			if !queryParam.Has("v") {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fifo.add(queueName, queryParam.Get("v"))
		case http.MethodGet:
			elem, err := fifo.get(queueName)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write([]byte(elem))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
	mux.HandleFunc("/", handler)
}

type httpFifo struct {
	storage map[string][]*string
	mutex   sync.Mutex
}

func newHttpFifo() *httpFifo {
	return &httpFifo{
		storage: make(map[string][]*string),
	}
}

func (h *httpFifo) add(queueName, val string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	slice, found := h.storage[queueName]
	if !found {
		newQueue := make([]*string, 0, 10)
		newQueue = append(newQueue, &val)
		h.storage[queueName] = newQueue
		return
	}
	h.storage[queueName] = append(slice, &val)
}

func (h *httpFifo) get(queueName string) (string, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	slice, found := h.storage[queueName]
	if !found {
		return "", errNotInitializedQueue
	}
	if len(slice) == 0 {
		return "", errEmptyQueue
	}
	firstElem := *slice[0]
	h.storage[queueName] = slice[1:]
	return firstElem, nil
}
