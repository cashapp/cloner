FROM golang:1.13-alpine as builder

RUN apk add --no-cache \
    wget \
    make \
    git \
    gcc \
    musl-dev

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

RUN mkdir -p /go/src/github.com/pingcap
WORKDIR /go/src/github.com/pingcap

RUN git clone https://github.com/bezmax/tidb.git
RUN git clone https://github.com/bezmax/tipb.git
RUN git clone https://github.com/pingcap/parser.git

WORKDIR /go/src/github.com/pingcap/tidb
RUN git checkout bezmax-vitess-hash
WORKDIR /go/src/github.com/pingcap/tipb
RUN git checkout bezmax-vitess-hash

WORKDIR /go/src/github.com/pingcap/tidb
RUN GO111MODULE=on go mod download
RUN go mod edit -replace=github.com/pingcap/tipb=../tipb
RUN go mod edit -replace=github.com/pingcap/parser=../parser
RUN make

# Executable image
FROM alpine

RUN apk add --no-cache \
    curl

COPY --from=builder /go/src/github.com/pingcap/tidb/bin/tidb-server /tidb-server
COPY --from=builder /usr/local/bin/dumb-init /usr/local/bin/dumb-init

WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/usr/local/bin/dumb-init", "/tidb-server"]