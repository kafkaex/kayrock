# Kayrock

WIP towards a Kafka client with better Kafka protocol support.

## Basic idea

So far I've only implemented a serializer, so this is only half of the story.

See [lib/kayrock/produce.ex](lib/kayrock/produce.ex) for an implementation of
the produce API.  There is a macro called `Kayrock.Generate.build_modules/1`
that, given the symbol name of the API, gets the message schema from
`:kpro_schema` (the `kafka_protocol` repo) and:

1. Generates a module for each version of the schema - e.g., `Kayrock.Produce.V0`, etc.
2. Each module has a struct matching that version of the message.
3. Each module has a simple API with some metadata and a serialize function
4. Generates an implementation for the `Kayrock.Request` protocol

## Give it a whirl

```elixir
iex(1)> {:ok, pid} = Kayrock.Client.start_link

14:06:02.169 [debug] Connecting to seed broker "localhost":9092
 
14:06:02.190 [debug] Successfully connected to broker 'localhost':9092 (#PID<0.215.0>)
 
14:06:02.195 [debug] Disconnected from seed broker #PID<0.215.0>
{:ok, #PID<0.214.0>}

14:06:02.196 [debug] Successfully connected to broker {10, 0, 1, 21}:9092 (#PID<0.218.0>)
iex(2)> Kayrock.Client.broker_call(pid, 0, %Kayrock.ApiVersions.V0.Request{})
{:ok,
 %Kayrock.ApiVersions.V0.Response{
   api_versions: [
     %{api_key: 0, max_version: 3, min_version: 0},
     %{api_key: 1, max_version: 5, min_version: 0},
     %{api_key: 2, max_version: 2, min_version: 0},
     %{api_key: 3, max_version: 4, min_version: 0},
     %{api_key: 4, max_version: 0, min_version: 0},
     %{api_key: 5, max_version: 0, min_version: 0},
     %{api_key: 6, max_version: 3, min_version: 0},
     %{api_key: 7, max_version: 1, min_version: 1},
     %{api_key: 8, max_version: 3, min_version: 0},
     %{api_key: 9, max_version: 3, min_version: 0},
     %{api_key: 10, max_version: 1, min_version: 0},
     %{api_key: 11, max_version: 2, min_version: 0},
     %{api_key: 12, max_version: 1, min_version: 0},
     %{api_key: 13, max_version: 1, min_version: 0},
     %{api_key: 14, max_version: 1, min_version: 0},
     %{api_key: 15, max_version: 1, min_version: 0},
     %{api_key: 16, max_version: 1, min_version: 0},
     %{api_key: 17, max_version: 0, min_version: 0},
     %{api_key: 18, max_version: 1, min_version: 0},
     %{api_key: 19, max_version: 2, min_version: 0},
     %{api_key: 20, max_version: 1, min_version: 0},
     %{api_key: 21, max_version: 0, min_version: 0},
     %{api_key: 22, max_version: 0, min_version: 0},
     %{api_key: 23, max_version: 0, min_version: 0},
     %{api_key: 24, max_version: 0, min_version: 0},
     %{api_key: 25, max_version: 0, min_version: 0},
     %{api_key: 26, max_version: 0, min_version: 0},
     %{api_key: 27, max_version: 0, min_version: 0},
     %{api_key: 28, max_version: 0, min_version: 0},
     %{api_key: 29, max_version: 0, min_version: 0},
     %{api_key: 30, max_version: 0, min_version: 0},
     %{api_key: 31, max_version: 0, min_version: 0},
     %{api_key: 32, max_version: 0, min_version: 0},
     %{api_key: 33, max_version: 0, min_version: 0}
   ],
   correlation_id: 0,
   error_code: 0
 }}
 ```

 ## TODO

 * Retry if more than one seed broker is listed and we fail
 * Better connection error handling
 * Use ISRs to allow for failure
