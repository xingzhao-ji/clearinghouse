.PHONY: build test demo services
build:
	go build -o .runtime/clearinghouse ./cmd/clearinghouse
test:
	go test -race -count=1 ./...
demo: build
	python3 scripts/demo.py
services:
	./scripts/services.sh up
