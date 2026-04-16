version = $(shell git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')
version := $(if $(version),$(version),dev)

.PHONY: build
build:
	CGO_ENABLED=0 go build -o base-ha .

.PHONY: test
test:
	go test ./...

.PHONY: docker-image
docker-image:
	docker build . -t ghcr.io/hanzoai/base-ha:$(version) --target production

.PHONY: release
release:
	goreleaser release --clean
