FROM rust:1.85 AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/battery-agg-rs /battery-agg-rs
USER nonroot:nonroot
ENTRYPOINT ["/battery-agg-rs"]
