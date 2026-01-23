# Contributing to Kayrock

Thank you for your interest in contributing to Kayrock!

## Development Setup

### Prerequisites

- Elixir 1.14 or later
- Erlang/OTP 24.3 or later
- Docker (for integration tests)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/kafkaex/kayrock.git
cd kayrock

# Install dependencies
mix deps.get

# Run tests
mix test

# Run static analysis
mix credo --strict
mix dialyzer
```

## Code Generation

Kayrock uses code generation from `kafka_protocol` schemas. The generated code is in `lib/generated/`.

**Important:** Do not edit files in `lib/generated/` manually.

To regenerate the protocol code:

```bash
mix gen.kafka_protocol
```

This is only needed when:
- Updating the `kafka_protocol` dependency
- Adding support for new Kafka protocol versions

## Running Tests

### Unit Tests

```bash
mix test
```

### Integration Tests

Integration tests require a Kafka cluster. We use [testcontainers](https://github.com/testcontainers/testcontainers-elixir):

```bash
# Requires Docker to be running
mix test.integration
```

## Code Quality Requirements

All contributions must pass:

1. **Compilation without warnings**
   ```bash
   mix compile --warnings-as-errors
   ```

2. **Unit tests**
   ```bash
   mix test
   ```

3. **Credo (code style)**
   ```bash
   mix credo --strict
   ```

4. **Dialyzer (type checking)**
   ```bash
   mix dialyzer
   ```

5. **Code formatting**
   ```bash
   mix format --check-formatted
   ```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Ensure all checks pass (see above)
5. Commit with a descriptive message
6. Push to your fork
7. Open a Pull Request

### Commit Messages

Use clear, descriptive commit messages:

```
Add support for Kafka 3.x protocol changes

- Updated FetchRequest to support new fields
- Added tests for new behavior
- Updated documentation
```

### PR Description

Include:
- What the change does
- Why it's needed
- Any breaking changes
- Related issues (if applicable)

## Project Structure

```
kayrock/
├── lib/
│   ├── kayrock/           # Core library code
│   │   ├── client.ex      # Kafka client (development only)
│   │   ├── compression.ex # Compression handling
│   │   ├── record_batch.ex# Record batch serialization
│   │   └── ...
│   ├── generated/         # Auto-generated protocol code
│   └── mix/tasks/         # Mix tasks (code generation)
├── test/
│   ├── kayrock/           # Unit tests
│   ├── integration/       # Integration tests
│   └── support/           # Test helpers
└── config/                # Configuration
```

## LLM/AI Integration

Kayrock includes a `usage-rules.md` file that provides guidelines for LLM-based coding assistants.
This file is automatically available to tools that support the [usage_rules](https://hex.pm/packages/usage_rules) package.

### For users of Kayrock

If you use LLM coding assistants (Claude, Cursor, etc.), you can pull in Kayrock's usage rules:

```bash
# Add usage_rules to your project
mix igniter.install usage_rules

# Sync Kayrock's rules to your AI config file
mix usage_rules.sync CLAUDE.md kayrock --link-to-folder deps
```

### For contributors

When updating `usage-rules.md`, ensure it includes:
- Common mistakes and how to avoid them
- Correct usage patterns with code examples
- API-specific guidance

## Questions?

- Open an issue for bugs or feature requests
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
