build:
	@CGO_ENABLED=0 go build -o bin/nxfs

run-master: build
	@./bin/nxfs -type master -tcp :8000 -http :8080

run-data1: build
	@./bin/nxfs -type data -path ./bin/nfs -tcp :8001 -master :8000

run-data2: build
	@./bin/nxfs -type data -id data2 -tcp :8002 -master :8000

run-data3: build
	@./bin/nxfs -type data -id data3 -tcp :8003 -master :8000

run-all: build
	@./bin/nxfs -type master -id master1 -tcp :8000 -http :8080 & \
	./bin/nxfs -type data -id data1 -tcp :8001 -master :8000 & \
	./bin/nxfs -type data -id data2 -tcp :8002 -master :8000 & \
	./bin/nxfs -type data -id data3 -tcp :8003 -master :8000

stop-all:
	@pkill -f bin/nxfs

test:
	go test ./... -v

.PHONY: build run-master run-data1 run-data2 run-data3 run-all stop-all test