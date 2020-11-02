FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN apk add --update bash curl && rm -rf /var/cache/apk/*
RUN apk add --no-cache tzdata
ADD cloner .
CMD [ "cloner" ]
