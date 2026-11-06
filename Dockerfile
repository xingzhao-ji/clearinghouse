FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /clearinghouse ./cmd/clearinghouse
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /clearinghouse /clearinghouse
ENTRYPOINT ["/clearinghouse"]
CMD ["-mode","api","-addr","0.0.0.0:8089"]
