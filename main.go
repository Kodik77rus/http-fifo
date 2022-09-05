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
		storage: make(map[string][]*string),
		eventListener: &eventListener{
			listeners: make(map[string][]chan *string),
		},
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

				broker.add(queueName, queryParam.Get("v"))
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

				w.Write([]byte(*<-broker.get(queueName)))
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		})

	log.Fatal(server.ListenAndServe())
}

type queueBroker struct {
	storage       map[string][]*string
	eventListener *eventListener
	mutex         sync.Mutex
}

type eventListener struct {
	listeners map[string][]chan *string
}

func (q *queueBroker) add(queueName string, val string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.eventListener.emit(queueName, val) {
		return
	}

	if chanSlice, ok := q.storage[queueName]; ok {
		q.storage[queueName] = append(chanSlice, &val)
	}

	q.storage[queueName] = []*string{&val}
}

func (q *queueBroker) get(queueName string) <-chan *string {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	ch := make(chan *string, 1)

	if slice, ok := q.storage[queueName]; ok {
		if len(slice) > 0 {
			ch <- q.storage[queueName][0]
			close(ch)
			q.storage[queueName] = slice[1:]
			return ch
		}
	}

	q.eventListener.addListener(queueName, ch)
	return ch
}

func (e *eventListener) addListener(event string, ch chan *string) {
	if listeners, ok := e.listeners[event]; ok {
		e.listeners[event] = append(listeners, ch)
		return
	}

	e.listeners[event] = []chan *string{ch}
}

func (e *eventListener) emit(event string, val string) bool {
	if chans, ok := e.listeners[event]; ok {
		if len(chans) > 0 {
			go func(ch chan *string) {
				ch <- &val
				close(ch)
			}(chans[0])

			e.listeners[event] = chans[1:]
			return true
		}
	}

	return false
}

func getWithTimeout(
	queueName string,
	interval time.Duration,
	fn func(queueName string) <-chan *string,
) <-chan *string {
	ch := make(chan *string)

	go func() {
		defer close(ch)
		for {
			select {
			case elem := <-fn(queueName):
				ch <- elem
				return
			case <-time.After(interval):
				ch <- nil
				return
			}
		}
	}()

	return ch
}
