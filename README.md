# Kayrock

[![CI Checks](https://github.com/kafkaex/kayrock/actions/workflows/checks.yml/badge.svg?branch=master)](https://github.com/kafkaex/kayrock/actions/workflows/checks.yml)
[![CI Tests](https://github.com/kafkaex/kayrock/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/kafkaex/kayrock/actions/workflows/test.yml)
[![Module Version](https://img.shields.io/hexpm/v/kayrock.svg)](https://hex.pm/packages/kayrock)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/kayrock/)
[![Total Download](https://img.shields.io/hexpm/dt/kayrock.svg)](https://hex.pm/packages/kayrock)
[![License](https://img.shields.io/hexpm/l/kayrock.svg)](https://hex.pm/packages/kayrock)
[![Last Updated](https://img.shields.io/github/last-commit/kafkaex/kayrock.svg)](https://github.com/kafkaex/kayrock/commits/master)

Idiomatic Elixir interface to the Kafka protocol.

This library provides serialization and deserialization for all Kafka protocol messages,
based on [kafka_protocol](https://github.com/kafka4beam/kafka_protocol) for Erlang.
It is built to work with [KafkaEx](https://github.com/kafkaex/kafka_ex) though there
is no reason why it couldn't be used elsewhere.

## Installation

Add `kayrock` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kayrock, "~> 1.0"}
  ]
end
```

## Requirements

- Elixir 1.14 or later
- Erlang/OTP 24.3 or later

## Basic Architecture and Usage

Kayrock generates structs for every version of every message in the Kafka
protocol. It does this by converting the Erlang-based schema descriptions in
`:kpro_schema` from the `kafka_protocol` library and is therefore limited to
messages and versions included there.

Each request message has a serializer (that generates iodata), and each response
message has a deserializer. There is also a protocol, `Kayrock.Request`, that has
an implementation created for each request.

The generated structs are namespaced as follows:
`Kayrock.<API>.V<version>.[Request|Response]`

For example:
- `Kayrock.Fetch.V4.Request`
- `Kayrock.Produce.V1.Response`
- `Kayrock.Metadata.V1.Request`

### Example Usage

```elixir
alias Kayrock.Request

# Create a request
request = %Kayrock.ApiVersions.V1.Request{
  client_id: "my_client",
  correlation_id: 0
}

# Serialize to wire protocol (iodata)
wire_protocol = Request.serialize(request)

# ... send wire_protocol over your connection ...
# ... receive binary response ...

# Deserialize the response
response_deserializer = Request.response_deserializer(request)
{deserialized_resp, _rest} = response_deserializer.(binary_response)

# deserialized_resp is now a %Kayrock.ApiVersions.V1.Response{}
```

## Message Compression

Kayrock supports four compression formats:

| Format | Built-in | Dependency Required |
|--------|----------|---------------------|
| gzip | Yes | None |
| snappy | No | `{:snappyer, "~> 1.2"}` |
| lz4 | No | `{:lz4b, "~> 0.0.13"}` |
| zstd | OTP 27+ | `{:ezstd, "~> 1.0"}` (for OTP < 27) |

### Installing Compression Dependencies

Add the compression libraries you need to your `mix.exs`:

```elixir
def deps do
  [
    {:kayrock, "~> 1.0"},

    # Add compression libraries as needed:
    {:snappyer, "~> 1.2"},     # For Snappy compression
    {:lz4b, "~> 0.0.13"},      # For LZ4 compression
    {:ezstd, "~> 1.0"},        # For Zstandard (OTP < 27)
  ]
end
```

### Snappy Configuration

By default, Kayrock uses `snappyer` for Snappy compression. To use the legacy
`snappy` module instead:

```elixir
# config/config.exs
config :kayrock, snappy_module: :snappy
```

### Zstandard Support

Zstandard compression is available via:
1. **OTP 27+**: Native `:zstd` module (no dependency needed)
2. **OTP < 27**: Add `{:ezstd, "~> 1.0"}` to your dependencies

## Code Generation

This repo includes a mix task, `mix gen.kafka_protocol`, that produces the code
in `lib/generated`. The generated code is checked into the repository to
simplify usage. End users should not need to run the mix task unless they are
doing development on Kayrock itself.

## Relationship to Other Libraries

Kayrock uses only the `kpro_schema` module from `kafka_protocol`.
`kafka_protocol` provides quite a lot of functionality (especially when used as
part of `brod`). This repo limits the integration surface because:

1. `kafka_protocol` produces Erlang records whereas Kayrock provides Elixir structs
2. `kafka_protocol` provides network-level implementation whereas Kayrock only
   provides serialization and deserialization
3. `kpro_schema` is itself automatically generated from the Java source code of
   Kafka, meaning that Kayrock could feasibly reproduce this without needing an
   intermediate dependency

### Built-in Client (Development Only)

This repo includes a lightweight Kafka client implementation for development and
testing purposes.

> **Warning:** The built-in client (`Kayrock.Client`) is NOT production-ready.
> For production usage, please use:
> - [KafkaEx](https://github.com/kafkaex/kafka_ex)
> - [brod](https://github.com/kafka4beam/brod)

## Testing

Kayrock includes both unit tests and integration tests.

### Unit Tests

```bash
mix test
```

### Integration Tests

Integration tests require Docker. We use
[testcontainers](https://github.com/testcontainers/testcontainers-elixir)
to run a Kafka cluster.

```bash
# Requires Docker to be running
mix test.integration
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT License - see [LICENSE.txt](LICENSE.txt) for details.
