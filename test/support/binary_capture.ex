# credo:disable-for-this-file Credo.Check.Warning.IoInspect
# credo:disable-for-this-file Credo.Check.Refactor.FilterReject
# credo:disable-for-this-file Credo.Check.Refactor.MapJoin
defmodule Kayrock.Test.BinaryCapture do
  @moduledoc """
  Utility for capturing request/response binaries from a real Kafka broker.

  This module connects to Kafka, sends requests, and captures the exact binary
  data for use in unit tests.

  ## Usage

  Start Kafka first:

      docker compose up -d

  Then in IEx:

      iex -S mix
      alias Kayrock.Test.BinaryCapture
      BinaryCapture.capture_api_versions(0)  # Capture V0
      BinaryCapture.capture_all_api_versions()

  The output can be copy-pasted into test files.
  """

  @default_host ~c"localhost"
  @default_port 9092
  @default_client_id "kayrock-capture"

  # ============================================
  # Connection Helpers
  # ============================================

  @doc """
  Opens a TCP connection to Kafka.
  """
  def connect(host \\ @default_host, port \\ @default_port) do
    :gen_tcp.connect(host, port, [:binary, active: false, packet: 4])
  end

  @doc """
  Sends a request and receives the response.
  Returns {request_binary, response_binary}.
  """
  def send_request(socket, request_binary) when is_binary(request_binary) do
    :ok = :gen_tcp.send(socket, request_binary)

    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, response_binary} -> {:ok, response_binary}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes the connection.
  """
  def close(socket) do
    :gen_tcp.close(socket)
  end

  # ============================================
  # ApiVersions Capture
  # ============================================

  @doc """
  Captures ApiVersions request/response for a specific version.
  """
  def capture_api_versions(version, opts \\ []) do
    client_id = Keyword.get(opts, :client_id, @default_client_id)
    correlation_id = Keyword.get(opts, :correlation_id, version)

    request = build_api_versions_request(version, correlation_id, client_id)
    request_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {:ok, socket} = connect()
    {:ok, response_binary} = send_request(socket, request_binary)
    close(socket)

    # Deserialize to verify it works
    response_module = Module.concat([Kayrock.ApiVersions, :"V#{version}", Response])
    {response, rest} = response_module.deserialize(response_binary)

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("ApiVersions V#{version}")
    IO.puts(String.duplicate("=", 60))

    IO.puts("\n## Request Binary (#{byte_size(request_binary)} bytes)")
    print_binary(request_binary)

    IO.puts("\n## Response Binary (#{byte_size(response_binary)} bytes)")
    print_binary(response_binary)

    IO.puts("\n## Parsed Response")
    IO.inspect(response, pretty: true, limit: 20)

    if rest != <<>> do
      IO.puts("\n## Remaining bytes: #{byte_size(rest)}")
    end

    %{
      version: version,
      request: request_binary,
      response: response_binary,
      parsed: response
    }
  end

  defp build_api_versions_request(0, correlation_id, client_id) do
    %Kayrock.ApiVersions.V0.Request{
      correlation_id: correlation_id,
      client_id: client_id
    }
  end

  defp build_api_versions_request(1, correlation_id, client_id) do
    %Kayrock.ApiVersions.V1.Request{
      correlation_id: correlation_id,
      client_id: client_id
    }
  end

  defp build_api_versions_request(2, correlation_id, client_id) do
    %Kayrock.ApiVersions.V2.Request{
      correlation_id: correlation_id,
      client_id: client_id
    }
  end

  defp build_api_versions_request(3, correlation_id, client_id) do
    %Kayrock.ApiVersions.V3.Request{
      correlation_id: correlation_id,
      client_id: client_id,
      client_software_name: "kayrock",
      client_software_version: "1.0.0"
    }
  end

  @doc """
  Captures all ApiVersions versions.
  """
  def capture_all_api_versions(opts \\ []) do
    for version <- 0..3 do
      capture_api_versions(version, opts)
    end
  end

  # ============================================
  # Metadata Capture
  # ============================================

  @doc """
  Captures Metadata request/response for a specific version.
  """
  def capture_metadata(version, topics \\ [], opts \\ []) do
    client_id = Keyword.get(opts, :client_id, @default_client_id)
    correlation_id = Keyword.get(opts, :correlation_id, version)

    request = build_metadata_request(version, correlation_id, client_id, topics)
    request_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {:ok, socket} = connect()
    {:ok, response_binary} = send_request(socket, request_binary)
    close(socket)

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("Metadata V#{version} (topics: #{inspect(topics)})")
    IO.puts(String.duplicate("=", 60))

    IO.puts("\n## Request Binary (#{byte_size(request_binary)} bytes)")
    print_binary(request_binary)

    IO.puts("\n## Response Binary (#{byte_size(response_binary)} bytes)")
    print_binary(response_binary)

    # Try to deserialize
    response_module = Module.concat([Kayrock.Metadata, :"V#{version}", Response])

    try do
      {response, _rest} = response_module.deserialize(response_binary)
      IO.puts("\n## Parsed Response")
      IO.inspect(response, pretty: true, limit: 20)
    rescue
      e ->
        IO.puts("\n## Deserialization Error")
        IO.inspect(e)
    end

    %{version: version, request: request_binary, response: response_binary}
  end

  defp build_metadata_request(version, correlation_id, client_id, topics) do
    topic_structs = Enum.map(topics, fn name -> %{name: name} end)

    base = %{
      correlation_id: correlation_id,
      client_id: client_id,
      topics: topic_structs
    }

    module = Module.concat([Kayrock.Metadata, :"V#{version}", Request])

    fields =
      cond do
        version >= 8 ->
          Map.merge(base, %{
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false
          })

        version >= 4 ->
          Map.put(base, :allow_auto_topic_creation, false)

        true ->
          base
      end

    struct(module, fields)
  end

  # ============================================
  # Produce Capture
  # ============================================

  @doc """
  Captures Produce request/response for a specific version.
  Requires a topic to exist.
  """
  def capture_produce(version, topic, opts \\ []) do
    client_id = Keyword.get(opts, :client_id, @default_client_id)
    correlation_id = Keyword.get(opts, :correlation_id, version)
    key = Keyword.get(opts, :key, "test-key")
    value = Keyword.get(opts, :value, "test-value")

    request = build_produce_request(version, correlation_id, client_id, topic, key, value)
    request_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {:ok, socket} = connect()
    {:ok, response_binary} = send_request(socket, request_binary)
    close(socket)

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("Produce V#{version} (topic: #{topic})")
    IO.puts(String.duplicate("=", 60))

    IO.puts("\n## Request Binary (#{byte_size(request_binary)} bytes)")
    print_binary(request_binary)

    IO.puts("\n## Response Binary (#{byte_size(response_binary)} bytes)")
    print_binary(response_binary)

    %{version: version, request: request_binary, response: response_binary}
  end

  defp build_produce_request(version, correlation_id, client_id, topic, key, value)
       when version <= 2 do
    module = Module.concat([Kayrock.Produce, :"V#{version}", Request])

    message_set = %Kayrock.MessageSet{
      messages: [
        %Kayrock.MessageSet.Message{key: key, value: value}
      ]
    }

    struct(module, %{
      correlation_id: correlation_id,
      client_id: client_id,
      acks: 1,
      timeout: 5000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: 0, record_set: message_set}
          ]
        }
      ]
    })
  end

  defp build_produce_request(version, correlation_id, client_id, topic, key, value)
       when version >= 3 do
    module = Module.concat([Kayrock.Produce, :"V#{version}", Request])

    record_batch = %Kayrock.RecordBatch{
      attributes: 0,
      records: [
        %Kayrock.RecordBatch.Record{
          key: key,
          value: value,
          headers: [],
          attributes: 0,
          timestamp: -1,
          offset: 0
        }
      ]
    }

    struct(module, %{
      correlation_id: correlation_id,
      client_id: client_id,
      transactional_id: nil,
      acks: 1,
      timeout: 5000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: 0, record_set: record_batch}
          ]
        }
      ]
    })
  end

  # ============================================
  # FindCoordinator Capture
  # ============================================

  @doc """
  Captures FindCoordinator request/response.
  """
  def capture_find_coordinator(version, group_id, opts \\ []) do
    client_id = Keyword.get(opts, :client_id, @default_client_id)
    correlation_id = Keyword.get(opts, :correlation_id, version)

    request = build_find_coordinator_request(version, correlation_id, client_id, group_id)
    request_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {:ok, socket} = connect()
    {:ok, response_binary} = send_request(socket, request_binary)
    close(socket)

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("FindCoordinator V#{version} (group: #{group_id})")
    IO.puts(String.duplicate("=", 60))

    IO.puts("\n## Request Binary (#{byte_size(request_binary)} bytes)")
    print_binary(request_binary)

    IO.puts("\n## Response Binary (#{byte_size(response_binary)} bytes)")
    print_binary(response_binary)

    %{version: version, request: request_binary, response: response_binary}
  end

  defp build_find_coordinator_request(0, correlation_id, client_id, group_id) do
    %Kayrock.FindCoordinator.V0.Request{
      correlation_id: correlation_id,
      client_id: client_id,
      key: group_id
    }
  end

  defp build_find_coordinator_request(version, correlation_id, client_id, group_id)
       when version >= 1 do
    module = Module.concat([Kayrock.FindCoordinator, :"V#{version}", Request])

    struct(module, %{
      correlation_id: correlation_id,
      client_id: client_id,
      key: group_id,
      key_type: 0
    })
  end

  # ============================================
  # Generic Capture
  # ============================================

  @doc """
  Sends raw binary request and captures response.
  Useful for testing exact binary formats.
  """
  def capture_raw(request_binary) when is_binary(request_binary) do
    {:ok, socket} = connect()
    {:ok, response_binary} = send_request(socket, request_binary)
    close(socket)

    IO.puts("\n## Request Binary (#{byte_size(request_binary)} bytes)")
    print_binary(request_binary)

    IO.puts("\n## Response Binary (#{byte_size(response_binary)} bytes)")
    print_binary(response_binary)

    %{request: request_binary, response: response_binary}
  end

  # ============================================
  # Output Helpers
  # ============================================

  @doc """
  Prints binary in a format suitable for test files.
  """
  def print_binary(binary) do
    # Print as Elixir binary literal
    bytes = :binary.bin_to_list(binary)

    IO.puts("```elixir")
    IO.puts("<<")

    bytes
    |> Enum.chunk_every(16)
    |> Enum.with_index()
    |> Enum.each(fn {chunk, idx} ->
      hex =
        chunk
        |> Enum.map(&Integer.to_string(&1))
        |> Enum.join(", ")

      # Add ASCII preview as comment
      ascii =
        chunk
        |> Enum.map(fn
          b when b >= 32 and b < 127 -> <<b>>
          _ -> "."
        end)
        |> Enum.join()

      comma = if idx < div(length(bytes) - 1, 16), do: ",", else: ""
      IO.puts("  #{hex}#{comma}  # #{ascii}")
    end)

    IO.puts(">>")
    IO.puts("```")

    # Also print as hex string for reference
    hex_str = Base.encode16(binary, case: :lower)
    IO.puts("\nHex: #{hex_str}")
  end

  @doc """
  Formats binary as Elixir literal for copy-paste.
  """
  def to_elixir_binary(binary) do
    bytes = :binary.bin_to_list(binary)

    bytes
    |> Enum.chunk_every(12)
    |> Enum.map(fn chunk ->
      chunk
      |> Enum.map(&Integer.to_string(&1))
      |> Enum.join(", ")
    end)
    |> Enum.join(",\n  ")
    |> then(&"<<\n  #{&1}\n>>")
  end

  # ============================================
  # Verification
  # ============================================

  @doc """
  Verifies that our serialization matches what Kafka expects.
  Sends our serialized request and checks if we get a valid response.
  """
  def verify_request(request) do
    request_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {:ok, socket} = connect()
    result = send_request(socket, request_binary)
    close(socket)

    case result do
      {:ok, response_binary} ->
        IO.puts("Request accepted by Kafka!")
        IO.puts("Response size: #{byte_size(response_binary)} bytes")

        # Try to get the deserializer and parse
        deserializer = Kayrock.Request.response_deserializer(request)
        {response, rest} = deserializer.(response_binary)

        IO.puts("\nParsed response:")
        IO.inspect(response, pretty: true, limit: 20)

        if rest != <<>> do
          IO.puts("\nRemaining bytes: #{byte_size(rest)}")
        end

        {:ok, response}

      {:error, reason} ->
        IO.puts("Request failed: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
