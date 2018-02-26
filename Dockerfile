FROM golang:1.10
RUN go get -d -v github.com/rapidloop/pgmetrics
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /root/pgmetrics github.com/rapidloop/pgmetrics/cmd/pgmetrics

FROM alpine:latest  
RUN apk --no-cache add ca-certificates
COPY --from=0 /root/pgmetrics /bin/pgmetrics
ENTRYPOINT ["/bin/pgmetrics"]

