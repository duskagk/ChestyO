build:
	@go build -o bin/fs

run-master: build
	@./bin/fs -type master -id master1 -addr :8000

run-data1: build
	@./bin/fs -type data -id data1 -addr :8001 -master :8000

run-data2: build
	@./bin/fs -type data -id data2 -addr :8002 -master :8000

run-data3: build
	@./bin/fs -type data -id data3 -addr :8003 -master :8000

run-all: build
	@./bin/fs -type master -id master1 -addr :8000 & \
	./bin/fs -type data -id data1 -addr :8001 -master :8000 & \
	./bin/fs -type data -id data2 -addr :8002 -master :8000 & \
	./bin/fs -type data -id data3 -addr :8003 -master :8000

stop-all:
	@pkill -f bin/fs

test:
	go test ./... -v 

.PHONY: build run-master run-data1 run-data2 run-data3 run-all stop-all test