FROM golang:1.17.12-alpine3.16

WORKDIR /home/app/

# Dependencies
COPY ./go.mod ./go.mod
COPY ./go.sum ./go.sum
RUN go mod download

COPY ./botapi ./botapi
COPY ./clouddb ./clouddb
COPY main.go main.go
RUN go build -o app
CMD ./app