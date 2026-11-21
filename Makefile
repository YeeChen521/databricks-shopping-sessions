IMAGE    ?= shopping-sessions:latest
CPUS     ?= 8
MEM      ?= 24g
PLATFORM ?= linux/amd64
DEV      ?= dev
ANSWER   ?= dev-answer
CXXFLAGS ?= -O3 -march=x86-64-v3 -std=c++17 -pthread -static -Wall -Wextra
export IMAGE CPUS MEM PLATFORM

.PHONY: build native fixtures fixtures-native dev run-dev check-dev reference-dev fuzz-ts

## Build the submission image (linux/amd64)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Build the native binary directly (Linux, g++ >= 12)
native:
	mkdir -p build && g++ $(CXXFLAGS) -o build/ss src/ss.cpp

## Run the image on all 36 fixtures with the scorer's flags
fixtures: build
	python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- starter-duckdb/scripts/docker_run.sh

## Run the native binary on all 36 fixtures (no Docker)
fixtures-native: native
	python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- build/ss

## Generate the 1M-event dev set (needs `generate` on PATH; see generator/README.md)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run the image on the dev set with the scorer's flags and time it
run-dev: build
	rm -rf out && time starter-duckdb/scripts/docker_run.sh $(DEV) out

## Exact diff of out/ against the published dev answer
check-dev:
	python3 starter-duckdb/scripts/compare.py out $(ANSWER)

## Independent DuckDB reference on the dev set (slow; for cross-checking)
reference-dev:
	rm -rf out-ref && python3 reference/duckdb_pipeline.py $(DEV) out-ref && python3 starter-duckdb/scripts/compare.py out-ref $(ANSWER)

## Fuzz the timestamp parser against a Python implementation of the rules
fuzz-ts:
	g++ -O2 -march=x86-64-v3 -std=c++17 -DSS_TSTEST -o build/tstest src/ss.cpp && python3 tools/fuzz_ts.py build/tstest
