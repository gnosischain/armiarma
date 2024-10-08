FROM golang:1.22-alpine AS builder

WORKDIR /armiarma
RUN apk add --no-cache git make

# Clone the repository and initialize submodules
RUN git clone --recursive https://github.com/gnosischain/armiarma.git .
RUN git submodule update --init --recursive

RUN make dependencies
RUN make build

# FINAL STAGE -> copy the binary and few config files
FROM debian:buster-slim

RUN mkdir /crawler
COPY --from=builder /armiarma/build/ /crawler

# Crawler exposed Port
EXPOSE 9020 
# Crawler exposed Port for Prometheus data export
EXPOSE 9080

ENTRYPOINT ["/crawler/armiarma"]
