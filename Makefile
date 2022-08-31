build:
	go build -o bin/httpFifo ./main.go
clean:
	rm -f bin/httpFifo 
run:
	bin/httpFifo 
start:
	make build && make run
