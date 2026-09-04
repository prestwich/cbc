FROM rust:1.96-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && cp target/release/cbc /cbc

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /cbc /cbc
EXPOSE 3000
ENTRYPOINT ["/cbc"]
