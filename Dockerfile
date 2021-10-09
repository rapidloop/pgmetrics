FROM golang:1.17
RUN GO111MODULES=on CGO_ENABLED=0 GOOS=linux go install -v github.com/rapidloop/pgmetrics/cmd/pgmetrics@latest

FROM alpine:latest  
RUN apk --no-cache add ca-certificates
COPY --from=0 /go/bin/pgmetrics /bin/pgmetrics
ENTRYPOINT ["/bin/pgmetrics"]

