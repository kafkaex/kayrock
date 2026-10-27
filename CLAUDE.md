# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kayrock is an Elixir library for Kafka protocol serialization and deserialization. It generates Elixir structs from Kafka protocol schemas (Apache Kafka's JSON message definitions, plus `kafka_protocol`'s `kpro_schema` for the versions Kayrock has always generated) and provides encoders/decoders for all Kafka API messages.

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

# Run tests against a single-node KRaft Apache Kafka 4.4.0 broker (requires Docker)
mix test.kafka4

# Static analysis
mix credo --strict
mix dialyzer

# Format code
mix format

# Compile with warnings as errors
mix compile --warnings-as-errors

# Regenerate protocol code (after changing the generator or ApiTable)
mix gen.kafka_protocol

# Vendor Apache Kafka's JSON message specs for a release tag
mix kayrock.fetch_kafka_messages 4.4.0
```

## Architecture

### Code Generation Pipeline

1. `lib/mix/tasks/gen.kafka_protocol.ex` - Mix task that invokes code generation
2. `lib/kayrock/generate/api_table.ex` - **The** allowlist of APIs and the max version generated for each. Raise a max here to add a version.
3. `lib/kayrock/generate/schema_source.ex` - `kpro_schema`-shaped schema source. Versions `<= ApiTable.kpro_max_versions` come from `:kpro_schema` (so existing generated code stays byte-identical); higher versions come from the JSON specs. `validate!/0` runs before generation (rejects `latestVersionUnstable` versions, gaps, unknown APIs).
4. `lib/kayrock/generate/json_spec.ex` - Translates Apache Kafka JSON message specs (`priv/kafka_messages/<version>/`, vendored by `mix kayrock.fetch_kafka_messages`) into kpro-shaped schemas: versions/nullableVersions/flexibleVersions, `[]T`, nested structs, `commonStructs`, compact encodings, header versions, and named tagged fields (`taggedVersions` -> `{name, {:tagged_field, [tag:, type:, default:, nullable:]}}`, sorted by tag, just before `tagged_fields`).
5. `lib/kayrock/generate.ex` - Core macro-based code generation logic
6. Outputs generated modules to `lib/generated/`

After `mix gen.kafka_protocol`, `git diff lib/generated` must not touch versions that were already generated (kafka_ex pattern-matches on these structs). `mix gen.kafka_protocol --check` verifies the checked-in code matches the generator (CI enforces it).

### Schema types

Beyond kpro's types the generator understands `:uuid` (16-byte binary; `nil` serializes as the zero UUID, which deserializes as `<<0::128>>`), `:uint16`, `:float64` (NaN/Infinity as raw 8-byte binary), `:compact_records` (flexible record sets) and `{:nullable_struct, schema}` (int8 -1/1 presence marker).

Nested structs are decoded by `deserialize_field/4` clauses scoped by the struct's field name; when two nested structs in one response share a field name (e.g. StreamsGroupDescribe v1 `topology.subtopologies` / `topology_description.subtopologies`) they are scoped by path (`:"topology.subtopologies"`) instead. A required (non-nullable) top-level struct field defaults to `nil`, so its type is `nil | %{...}`.

Known `kpro_schema` quirk kept for compatibility: Fetch v7-v11 `forgotten_topics_data[].partitions` is modelled as an array of structs (Kafka defines `[]int32`).

### Tagged fields (KIP-482)

In JSON-derived versions, `taggedVersions` fields are named struct keys (default = the JSON `"default"`, see `JsonSpec.tagged_default/3`). The generator writes them into the struct's tagged-field section only when they differ from the default (`nil` means "not set" unless the field is nullable), merged in ascending tag order with any unknown tags in `tagged_fields`; a raw tag that duplicates a named one raises. On decode, known tags fill the named fields (absent ones get the default) and unknown tags stay in `tagged_fields`. kpro-sourced versions have no named tagged fields (their tags stay raw), so their structs are unchanged. Test-only specs for this live in `test/fixtures/kafka_messages/tagged_fields/`; `Kayrock.Test.GeneratedCode` compiles generator output in tests without touching `lib/generated`.

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
- `test/support/kafka4_container.ex` - Testcontainers builder for a single-node KRaft `apache/kafka:4.4.0` broker; `with_broker_config/2` (Kafka property overrides), `with_feature/3` (KRaft feature levels at format time, e.g. `share.version`), `with_release_version/2`
- `test/support/kafka4_case.ex` / `kafka4_helpers.ex` - `use Kayrock.Kafka4Case` (optionally `container: config`) tags tests `:kafka4` and imports `build_client/1`, `create_kafka4_topic/2`, `connect_broker/1`, `broker_api_versions/1`, `broker_supports?/3`
- `test/kafka4/` - the `:kafka4` suite (`mix test.kafka4`). `api_versions_smoke_test.exs` holds the list of generated versions a 4.4.0 broker no longer accepts (KIP-896); the broker sends no response at those versions (the connection is closed, which shows up as a recv timeout)

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
