FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN apk add --update bash curl && rm -rf /var/cache/apk/*
RUN apk add --no-cache tzdata
RUN addgroup -g 1000 appuser
RUN adduser -D -H -u 1000 -G appuser -g appuser -h /service appuser
WORKDIR /service
ADD cloner .

RUN chown appuser:appuser cloner && chmod 755 cloner
USER 1000
EXPOSE 8080 8443
ENV ENVIRONMENT="development"
CMD [ "./cloner" ]

