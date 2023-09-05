FROM golang:1.21 as build
COPY go.mod go.sum /src/
WORKDIR /src
RUN go mod download
ADD . /src
RUN make

FROM ubuntu:22.04
COPY --from=build /src/build/little_bigtable /usr/bin/little_bigtable
WORKDIR /app
RUN mkdir -p /app/data
COPY --from=build /src/build /app/
CMD ["./little_bigtable", "-host", "0.0.0.0", "-port", "9000", "-db-file", "/app/data/bigtable.db"]