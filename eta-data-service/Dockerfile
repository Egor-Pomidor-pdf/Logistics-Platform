FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY . .
ARG SERVICE_NAME
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/service ./cmd/${SERVICE_NAME}

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/service /app/service
CMD ["/app/service"]
