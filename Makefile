
build:
	docker run -v $(shell pwd)/cargo-config:/root/.cargo/config  -v $(shell pwd):/volume --rm -t clux/muslrust:stable cargo build --release

PHONY: build
