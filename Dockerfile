FROM golang:1.24-alpine AS build
WORKDIR /app
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o /bin/ls-load ./

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /data
COPY --from=build /bin/ls-load /usr/local/bin/ls-load
ENTRYPOINT ["ls-load"]
