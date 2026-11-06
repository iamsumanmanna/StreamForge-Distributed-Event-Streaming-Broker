FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /broker ./cmd/broker && \
    CGO_ENABLED=0 go build -o /produce ./cmd/produce && \
    CGO_ENABLED=0 go build -o /consume ./cmd/consume

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /broker /usr/local/bin/broker
COPY --from=build /produce /usr/local/bin/produce
COPY --from=build /consume /usr/local/bin/consume
EXPOSE 9092 8080 7000
ENTRYPOINT ["broker"]
