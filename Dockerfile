FROM golang:1.25.6-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o kvserver .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/kv .
EXPOSE 8080
ENTRYPOINT ["./kv"]