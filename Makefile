build:
	@go build -o bin/fs

run-master: build
	@./bin/fs -type master -id master1 -tcp :8000 -http :8080

run-data1: build
	@./bin/fs -type data -id data1 -tcp :8001 -master :8000

run-data2: build
	@./bin/fs -type data -id data2 -tcp :8002 -master :8000

run-data3: build
	@./bin/fs -type data -id data3 -tcp :8003 -master :8000

run-all: build
	@./bin/fs -type master -id master1 -tcp :8000 -http :8080 & \
	./bin/fs -type data -id data1 -tcp :8001 -master :8000 & \
	./bin/fs -type data -id data2 -tcp :8002 -master :8000 & \
	./bin/fs -type data -id data3 -tcp :8003 -master :8000

stop-all:
	@pkill -f bin/fs

test:
	go test ./... -v

.PHONY: build run-master run-data1 run-data2 run-data3 run-all stop-all test