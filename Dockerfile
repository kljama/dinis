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

RUN apk add --no-cache ca-certificates
COPY --from=builder /dinis /usr/local/bin/dinis

EXPOSE 8080

ENTRYPOINT ["dinis"]
