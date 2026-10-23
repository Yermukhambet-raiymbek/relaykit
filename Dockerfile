ARG GO_VERSION=1.25.3
FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o /out/relaykit ./cmd/relaykit

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home relay \
    && mkdir -p /data && chown relay:relay /data
COPY --from=build /out/relaykit /usr/local/bin/relaykit
USER 10001:10001
ENV RELAY_DB=/data/relaykit.db RELAY_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["relaykit"]
