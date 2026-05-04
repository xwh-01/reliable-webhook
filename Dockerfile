# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/reliable-webhook ./cmd/server

FROM alpine:3.22

RUN adduser -D -H -u 10001 appuser

WORKDIR /app

COPY --from=builder /out/reliable-webhook /app/reliable-webhook
COPY config.yml /app/config.yml

USER appuser

EXPOSE 8080

ENTRYPOINT ["/app/reliable-webhook"]
