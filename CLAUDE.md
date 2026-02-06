# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kayrock is an Elixir library for Kafka protocol serialization and deserialization. It generates Elixir structs from Kafka protocol schemas (via `kafka_protocol`'s `kpro_schema` module) and provides encoders/decoders for all Kafka API messages.

## Build and Development Commands

```bash
# Install dependencies
mix deps.get

# Run unit tests (excludes integration/chaos tests)
mix test
# or explicitly:
mix test.unit

# Run a single test file
mix test test/kayrock/generated/api_versions_test.exs

# Run a specific test by line number
mix test test/kayrock/generated/api_versions_test.exs:12

# Run integration tests (requires Docker)
mix test.integration

# Run chaos tests (requires Docker + Toxiproxy)
mix test.chaos

# Static analysis
mix credo --strict
mix dialyzer

# Format code
mix format

# Compile with warnings as errors
mix compile --warnings-as-errors

# Regenerate protocol code (only needed when updating kafka_protocol dep)
mix gen.kafka_protocol
```

## Architecture

### Code Generation Pipeline

1. `lib/mix/tasks/gen.kafka_protocol.ex` - Mix task that invokes code generation
2. `lib/kayrock/generate.ex` - Core macro-based code generation logic
3. Reads schemas from `:kpro_schema` (kafka_protocol dependency)
4. Outputs generated modules to `lib/generated/`

### Generated Module Structure

All generated structs follow the pattern: `Kayrock.<API>.V<version>.<Request|Response>`

Examples:
- `Kayrock.Produce.V3.Request`
- `Kayrock.Fetch.V4.Response`
- `Kayrock.ApiVersions.V1.Request`

Each API module also provides:
- `get_request_struct(version)` - Returns empty request struct for version
- `deserialize(version, binary)` - Deserializes response binary
- `min_vsn/0`, `max_vsn/0` - Version bounds

### Key Non-Generated Modules

- `lib/kayrock/request.ex` - Protocol defining `serialize/1`, `api_vsn/1`, `response_deserializer/1`
- `lib/kayrock/serialize.ex` - Serialization primitives (int8, int16, string, compact_string, etc.)
- `lib/kayrock/deserialize.ex` - Deserialization primitives
- `lib/kayrock/record_batch.ex` - Modern record batch format (Kafka 0.11+)
- `lib/kayrock/message_set.ex` - Legacy message set format (pre-0.11)
- `lib/kayrock/compression.ex` - Compression dispatch (gzip, snappy, lz4, zstd)
- `lib/kayrock/client.ex` - Development-only Kafka client (NOT production-ready)

### Test Infrastructure

- `test/support/test_support.ex` - Test helpers including `api_version_range/1` for each API
- `test/support/binary_capture.ex` - Utility for capturing real Kafka request/response binaries
- `test/support/factories/` - Request/response factories for each API
- `test/kayrock/generated/` - Unit tests for each generated API module

## Important Conventions

### DO NOT edit files in `lib/generated/`
These are auto-generated. Modify `lib/kayrock/generate.ex` instead.

### Flexible vs Standard Versions
Kafka protocol has "flexible" versions (KIP-482) that use compact encodings and tagged fields. The code generator handles this via `flexible_version?/2`.

### Test Factories Pattern
Each API has a factory in `test/support/factories/` that provides:
- `request_data(version)` - Returns `{request_struct, expected_binary}`
- `response_data(version)` - Returns `{response_binary, expected_struct}`
- Helper functions for edge cases

### Version Ranges
Use `Kayrock.TestSupport.api_version_range(:api_name)` to get supported versions for any API rather than hardcoding ranges.
