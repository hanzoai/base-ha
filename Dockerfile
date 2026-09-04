FROM golang:1.26.5-trixie AS builder
# go.mod pins the toolchain. The golang base image sets GOTOOLCHAIN=local,
# which turns a `go` directive newer than the image into a hard build
# failure instead of a download.
ENV GOTOOLCHAIN=auto

WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o base-ha .

# Production image

# One directory in an empty image: the static binary and its data directory.
# The entrypoint script did nothing but exec the binary, so the binary is the
# entrypoint.
FROM alpine:3.22 AS root
RUN apk add --no-cache ca-certificates tzdata && mkdir -p /app/data && chown -R 1000:1000 /app

FROM scratch
LABEL org.opencontainers.image.source=https://github.com/hanzoai/base-ha
COPY --from=root /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=root /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=root --chown=1000:1000 /app /app
COPY --from=builder /build/base-ha /app/base-ha
WORKDIR /app
VOLUME /app/data
EXPOSE 4222 6222 8090
USER 1000:1000
ENV BASE_PUBSUB_STORE_DIR="/app/data"
ENTRYPOINT ["/app/base-ha", "serve", "--http", "0.0.0.0:8090"]
