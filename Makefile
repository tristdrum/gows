all: clean build-proto test build

test:
	cd src && \
	go test ./...

clean:
	rm -rf src/proto
	rm -rf bin

build-proto:
	mkdir -p src/proto
	protoc \
		-I=. \
		--go_out=./src/proto \
		--go-grpc_out=./src/proto \
		--experimental_allow_proto3_optional \
		 proto/*.proto

tidy: build-proto
	cd src && \
	go mod tidy

build:
	cd src && \
	go build -o ../bin/gows .
