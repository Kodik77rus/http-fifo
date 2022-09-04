package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

func main() {
	port := flag.String("port", "8080", "server port")

	flag.Parse()

	broker := &queueBroker{
		storage: make(map[string][]chan string),
	}

	server := http.Server{
		Addr: fmt.Sprint(":", *port),
	}

	server.Handler = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			queueName := strings.TrimLeft(r.URL.Path, "/")
			queryParam := r.URL.Query()

			switch r.Method {
			case http.MethodPut:
				if !queryParam.Has("v") {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				broker.sendToListeners(queueName, queryParam.Get("v"))
			case http.MethodGet:
				if queryParam.Has("timeout") {
					sec, err := time.ParseDuration(queryParam.Get("timeout") + "s")
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}

					elem := <-getWithTimeout(queueName, sec, broker.get)
					if elem == nil {
						w.WriteHeader(http.StatusNotFound)
						return
					}

					w.Write([]byte(*elem))
					return
				}

				w.Write([]byte(<-broker.get(queueName)))
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		})

	log.Fatal(server.ListenAndServe())
}

type queueBroker struct {
	storage map[string][]chan string
	mutex   sync.Mutex
}

func (q *queueBroker) sendToListeners(queueName string, val string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if chanSlice, ok := q.storage[queueName]; ok {
		if len(chanSlice) > 0 {
			for _, ch := range chanSlice {
				if len(ch) == 0 {
					ch <- val
					close(ch)
					return
				}
			}
		}
		q.storage[queueName] = append(chanSlice, makeAndFillStringChan(val))
	}
	q.storage[queueName] = []chan string{makeAndFillStringChan(val)}
}

func (q *queueBroker) get(queueName string) <-chan string {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if slice, ok := q.storage[queueName]; ok {
		if len(slice) > 0 {
			ch := q.storage[queueName][0]
			if len(ch) == 0 {
				ch := make(chan string, 1)
				q.storage[queueName] = append(q.storage[queueName], ch)
				return ch
			}

			q.storage[queueName] = slice[1:]
			return ch
		}

		ch := make(chan string, 1)
		q.storage[queueName] = append(q.storage[queueName], ch)
		return ch
	}

	ch := make(chan string, 1)
	q.storage[queueName] = []chan string{ch}
	return ch
}

func getWithTimeout(
	queueName string,
	interval time.Duration,
	fn func(queueName string) <-chan string,
) <-chan *string {
	ch := make(chan *string)

	go func() {
		defer close(ch)

		for {
			select {
			case elem := <-fn(queueName):
				ch <- &elem
				return
			case <-time.After(interval):
				ch <- nil
				return
			}
		}
	}()
	return ch
}

func makeAndFillStringChan(val string) chan string {
	ch := make(chan string, 1)
	ch <- val
	close(ch)
	return ch
}
