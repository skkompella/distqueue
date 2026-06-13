# Multi-stage build: compile static binaries, ship a minimal image.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/broker /out/worker /out/produce /usr/local/bin/
ENTRYPOINT ["broker"]
