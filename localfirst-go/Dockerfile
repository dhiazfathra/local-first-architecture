# One image, both binaries. CGO_ENABLED=0 is not an optimisation here: the
# SQLite driver is pure Go (modernc.org/sqlite) precisely so this stays true.
FROM golang:1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node && \
    CGO_ENABLED=0 go build -o /out/central ./cmd/central

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/node /node
COPY --from=build /out/central /central
