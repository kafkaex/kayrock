# Kayrock Usage Rules

Kayrock is an Elixir library for Kafka protocol serialization and deserialization.
It generates Elixir structs from Kafka protocol schemas.

## Critical: The Built-in Client is NOT Production-Ready

The `Kayrock.Client` module and convenience functions like `Kayrock.produce/5` and
`Kayrock.fetch/5` are for **development and testing only**.

### DON'T use in production:

```elixir
# DON'T - Not production-ready
{:ok, client} = Kayrock.Client.start_link([{"localhost", 9092}])
Kayrock.produce(client, batch, "topic", 0)
```

### DO use Kayrock for serialization with a production client:

```elixir
# DO - Use with KafkaEx or brod
request = %Kayrock.Produce.V3.Request{
  acks: -1,
  timeout: 5000,
  topic_data: [%{topic: "my-topic", data: [...]}]
}
wire_data = Kayrock.Request.serialize(request)
# Send via KafkaEx or brod connection
```

## Struct Naming Convention

All generated structs follow this pattern:

```
Kayrock.<API>.V<version>.<Request|Response>
```

Examples:
- `Kayrock.Produce.V1.Request` - Produce API version 1 request
- `Kayrock.Fetch.V4.Response` - Fetch API version 4 response
- `Kayrock.Metadata.V1.Request` - Metadata API version 1 request

## Compression Dependencies

Compression support requires optional dependencies. Without them, you'll get runtime errors.

### Available compression formats:

| Format | Attribute | Dependency |
|--------|-----------|------------|
| None | 0 | Built-in |
| Gzip | 1 | Built-in |
| Snappy | 2 | `{:snappyer, "~> 1.2"}` |
| LZ4 | 3 | `{:lz4b, "~> 0.0.13"}` |
| Zstandard | 4 | `{:ezstd, "~> 1.0"}` |

### Add to mix.exs if using compression:

```elixir
{:snappyer, "~> 1.2"},   # For Snappy
{:lz4b, "~> 0.0.13"},    # For LZ4
{:ezstd, "~> 1.0"},      # For Zstandard
```

## Creating Record Batches

Use `Kayrock.RecordBatch` for modern Kafka (0.11+):

```elixir
batch = %Kayrock.RecordBatch{
  attributes: 0,  # 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
  records: [
    %Kayrock.RecordBatch.Record{
      key: "my-key",
      value: "my-value",
      headers: [{"header-name", "header-value"}]
    }
  ]
}
```

## Serialization and Deserialization

### Serialize a request:

```elixir
request = %Kayrock.Metadata.V1.Request{
  correlation_id: 1,
  client_id: "my_app",
  topics: [%{name: "my-topic"}]
}

# Returns iodata (efficient for network)
wire_data = Kayrock.Request.serialize(request)
```

### Deserialize a response:

```elixir
# Get the deserializer for a request
deserializer = Kayrock.Request.response_deserializer(request)

# Parse binary response
{response, _rest} = deserializer.(binary_response)
```

## API Version Selection

Different Kafka versions support different API versions. Always check broker compatibility.

### Produce API versions:

| Version | Min Kafka | Features |
|---------|-----------|----------|
| V0-V2 | 0.9 | Basic produce |
| V3 | 0.11 | Idempotent producer |
| V7 | 2.1 | Transaction support |

### Fetch API versions:

| Version | Min Kafka | Features |
|---------|-----------|----------|
| V0-V3 | 0.9 | Basic fetch |
| V4 | 0.11 | Isolation level |
| V7 | 1.1 | Session fetch |

## Error Handling

Kafka errors are returned in response structs, not as exceptions.

```elixir
case response do
  %{error_code: 0} ->
    # Success
    :ok

  %{error_code: error_code} ->
    # Use Kayrock.ErrorCode to decode
    error_name = Kayrock.ErrorCode.code_to_atom(error_code)
    {:error, error_name}
end
```

## Common Mistakes to Avoid

1. **Don't use the built-in client in production** - Use KafkaEx or brod
2. **Don't forget compression dependencies** - They're optional, add what you need
3. **Don't hardcode API versions** - Check broker capabilities with ApiVersions
4. **Don't ignore error_code in responses** - Kafka returns errors in the response
5. **Don't assume message ordering** - Use partition keys for ordering guarantees

## Working with Multiple Partitions

```elixir
# Produce to specific partition
request = %Kayrock.Produce.V1.Request{
  topic_data: [
    %{
      topic: "my-topic",
      data: [
        %{partition: 0, record_set: batch_for_partition_0},
        %{partition: 1, record_set: batch_for_partition_1}
      ]
    }
  ]
}
```

## Quick Reference

### Check broker API versions:

```elixir
request = %Kayrock.ApiVersions.V1.Request{
  correlation_id: 1,
  client_id: "my_app"
}
```

### Fetch topic metadata:

```elixir
request = %Kayrock.Metadata.V1.Request{
  correlation_id: 1,
  client_id: "my_app",
  topics: [%{name: "my-topic"}]  # or nil for all topics
}
```

### Create topics:

```elixir
request = %Kayrock.CreateTopics.V2.Request{
  correlation_id: 1,
  client_id: "my_app",
  create_topic_requests: [
    %{
      topic: "new-topic",
      num_partitions: 3,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: []
    }
  ],
  timeout: 30_000
}
```
