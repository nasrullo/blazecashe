FROM rust:1-bookworm AS builder

WORKDIR /app
COPY . .
# Build the main server binary
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Copy the release binary from the builder stage
COPY --from=builder /app/target/release/blazecache /usr/local/bin/blazecache

EXPOSE 6784 6785

CMD ["blazecache"]
