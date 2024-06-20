FROM golang:1.22.3-alpine3.20

ENV GOPATH=/
RUN go env -w GOCACHE=/.cache

COPY ./ ./

RUN go mod download
RUN --mount=type=cache,target=/.cache go build -v -o dbb-proxy ./cmd/proxy

ENTRYPOINT ./dbb-proxy
