# Use a Rust base image
FROM rust:latest as builder

# Install CMake
RUN apt update && \
    apt install -y cmake protobuf-compiler libprotobuf-dev&& \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the source code
COPY . .

# Build the Rust project
RUN cargo build --release

# Create a new, smaller image without the build dependencies
FROM ubuntu:23.04

WORKDIR /app

# Copy just the compiled binary from the previous stage
COPY --from=builder /app/target/release/edcas-api /app/

# Set the entry point
CMD ["/app/edcas-api"]