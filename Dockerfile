# Stage 1: Build
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /dinis .

# Stage 2: Runtime
FROM alpine:3.20

RUN apk add --no-cache ca-certificates iputils tzdata \
    && mkdir -p /data

COPY --from=builder /dinis /usr/local/bin/dinis

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget -q -O - http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["dinis"]
