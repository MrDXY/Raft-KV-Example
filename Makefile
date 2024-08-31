BINARY_NAME=raft-kv-example

# default target
.PHONY: all
all: build

build:
	@echo "Building the server..."
	go build -o $(BINARY_NAME) .

run-single: build
	@echo "Starting single node ..."
	./$(BINARY_NAME) --id=1 --peer-port=2380 --client-port=2379 --peers="http://localhost:2380" > node-single.log 2>&1 & echo $$! > node-single.pid

stop-single:
	@echo "Stopping single node ..."
	@if [ -f node-single.pid ]; then kill $$(cat node-single.pid); rm node-single.pid; fi

run-node1: build
	@echo "Starting node 1..."
	./$(BINARY_NAME) --id=1 --peer-port=2380 --client-port=2379 --peers="http://localhost:2380,http://localhost:12380,http://localhost:22380" > node1.log 2>&1 & echo $$! > node1.pid

run-node2: build
	@echo "Starting node 2..."
	./$(BINARY_NAME) --id=2 --peer-port=12380 --client-port=12379 --peers="http://localhost:2380,http://localhost:12380,http://localhost:22380" > node2.log 2>&1 & echo $$! > node2.pid

run-node3: build
	@echo "Starting node 3..."
	./$(BINARY_NAME) --id=3 --peer-port=22380 --client-port=22379 --peers="http://localhost:2380,http://localhost:12380,http://localhost:22380" > node3.log 2>&1 & echo $$! > node3.pid

.PHONY: run-all
run-all: run-node1 run-node2 run-node3
	@echo "All nodes are started."

stop-node1:
	@echo "Stopping node 1..."
	@if [ -f node1.pid ]; then kill $$(cat node1.pid); rm node1.pid; fi

stop-node2:
	@echo "Stopping node 2..."
	@if [ -f node2.pid ]; then kill $$(cat node2.pid); rm node2.pid; fi

stop-node3:
	@echo "Stopping node 3..."
	@if [ -f node3.pid ]; then kill $$(cat node3.pid); rm node3.pid; fi

.PHONY: stop-all
stop-all: stop-node1 stop-node2 stop-node3
	@echo "All nodes are stopped."

.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -f $(BINARY_NAME) node*.pid node*.log
	rm -rf data-*

.PHONY: put
put-node:
	curl -X PUT http://localhost:$(PORT)/kv -d "$(DATA)"

.PHONY: get
get-node:
	curl -X GET http://localhost:$(PORT)/kv -d "$(DATA)"

.PHONY: benchmark
benchmark:
	@echo "Starting benchmark..."
	for i in {1..100}; do \
		curl -X PUT http://localhost:2379/kv -d "key$$i=val$$i"; \
	done
	@echo "Benchmark completed."
