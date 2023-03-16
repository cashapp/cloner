FROM golang:1.20 AS builder
COPY . /go/src/github.com/cashapp/cloner
WORKDIR /go/src/github.com/cashapp/cloner
RUN CGO_ENABLED=0 go build -o cloner ./cmd/cloner

FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN apk add --update bash curl && rm -rf /var/cache/apk/*
RUN apk add --no-cache tzdata
COPY --from=builder /go/src/github.com/cashapp/cloner/cloner /
ENTRYPOINT [ "/cloner" ]
