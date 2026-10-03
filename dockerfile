# Use the same major.minor as the "go" line in go.mod.
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/seatres ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/seatres /seatres
EXPOSE 8080
ENTRYPOINT ["/seatres"]