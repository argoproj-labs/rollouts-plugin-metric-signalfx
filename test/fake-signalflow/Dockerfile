FROM golang:1.26 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fake-signalflow ./test/fake-signalflow

FROM scratch

COPY --from=build /out/fake-signalflow /fake-signalflow
EXPOSE 8080
ENTRYPOINT ["/fake-signalflow"]
