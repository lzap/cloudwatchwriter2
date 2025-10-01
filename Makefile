.PHONY: all build test bench clean lint

BINARY_NAME=cloudwatchwriter2

all: fmt test bench

build:
	go build -o $(BINARY_NAME) .

fmt:
	go fmt .

test:
	go test -v .

bench:
	go test -bench=. -benchmem -benchtime=100ms -run=^Test .

clean:
	rm -f $(BINARY_NAME)

lint:
	staticcheck .
