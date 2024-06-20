.PHONY: build
build:
	GOCACHE=`pwd`/.cache go build -v -o dbb-proxy ./cmd/proxy

.DEFAULT_GOAL = build