# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - YYYY-MM-DD

First stable release of Kayrock.

### Added
- Comprehensive Kafka protocol support (44+ APIs)
- Full documentation with usage examples
- CONTRIBUTING.md for contributors

### Changed
- **BREAKING:** Compression libraries are now optional dependencies
  - Add `{:snappyer, "~> 1.2"}` for Snappy compression
  - Add `{:lz4b, "~> 0.0.13"}` for LZ4 compression
  - Add `{:ezstd, "~> 1.0"}` for Zstandard (OTP < 27)
- **BREAKING:** Minimum Elixir version bumped to 1.14
- **BREAKING:** Default Snappy module changed from `:snappy` to `:snappyer`
- Improved error messages for missing compression dependencies
- Fixed application config key from `:kafka_ex` to `:kayrock` for snappy module

### Fixed
- Snappy module configuration now correctly uses `:kayrock` app (was `:kafka_ex`)

### Deprecated
- `Kayrock.Client` - Use KafkaEx or brod for production clients

### Migration from 0.3.x

1. **Compression Dependencies**

   If you use compression, add the required dependency to your `mix.exs`:

   ```elixir
   defp deps do
     [
       {:kayrock, "~> 1.0"},
       {:snappyer, "~> 1.2"},  # If using Snappy
       {:lz4b, "~> 0.0.13"},   # If using LZ4
       {:ezstd, "~> 1.0"},     # If using Zstandard on OTP < 27
     ]
   end
   ```

2. **Elixir Version**

   Ensure you're running Elixir 1.14 or later.

3. **Snappy Configuration**

   If you were configuring the snappy module, update the config key:

   ```elixir
   # Before (broken)
   config :kafka_ex, snappy_module: :snappyer

   # After (correct)
   config :kayrock, snappy_module: :snappyer
   ```

   Note: The default is now `:snappyer`, so you only need this config if using `:snappy`.

4. **Client Usage**

   If using `Kayrock.Client` in production, migrate to KafkaEx or brod.
   The built-in client remains available for development/testing.

## [0.3.0] - 2025-12-11

### Added
- Zstandard compression support (#43)
- LZ4 compression support (#38)
- Native OTP 27+ Zstandard support with ezstd fallback
- Consumer group integration tests (#33)

### Changed
- Updated `kafka_protocol` from 2.4.1 to 4.3.1 (#42)
- Bumped minimum Elixir version to 1.12 (#34)
- Updated `varint` to 1.5.1 (#37)
- Migrating integration tests to testcontainers

### Fixed
- Compression tests (#40)
- GitHub Actions configuration (#39)
- Missing tests and dependencies (#36)

## [0.2.0] - 2024-11-29

### Added
- Consumer group integration tests (#33)
- Produce integration tests (#31)
- Integration tests (#28)
- Formatter and dialyzer setup

### Changed
- Updated crc and connection dependencies
- Updated varint dependency
- Updated CI workflows

### Fixed
- Handle incomplete records when parsing record batch (#24)
- Handle empty membership assignment response (#22)
- Fixed deprecation warnings (#29)
- Produce fixes and code regeneration (#32)

## [0.1.0] - Initial Release

### Added
- Initial release with Kafka protocol serialization/deserialization
- Generated structs for all Kafka API versions
- Basic client implementation (development use only)
- Gzip and Snappy compression support
- SSL/TLS connection support
- Record headers support (Kafka 0.11+)
- Compact encoding support (KIP-482)
- Tagged fields support

---

[Unreleased]: https://github.com/kafkaex/kayrock/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/kafkaex/kayrock/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/kafkaex/kayrock/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/kafkaex/kayrock/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/kafkaex/kayrock/releases/tag/v0.1.0
