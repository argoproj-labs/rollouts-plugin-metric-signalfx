.PHONY: fmt test vet build clean

fmt:
	gofmt -w main.go rpc_test.go rpc_binary_test.go internal/plugin/*.go test/fake-signalflow/*.go

test:
	go test -race ./...

vet:
	go vet ./...

build:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/metric-plugin-linux-amd64 .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/metric-plugin-linux-arm64 .

clean:
	rm -rf dist
