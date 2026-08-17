# Build stage
FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /jobqueue ./cmd/jobqueue

# Runtime stage
FROM gcr.io/distroless/static:nonroot
COPY --from=build /jobqueue /jobqueue
USER nonroot:nonroot
ENTRYPOINT ["/jobqueue"]