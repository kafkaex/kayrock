defmodule Kayrock.Sanity.TopicSanityTest do
  @moduledoc """
  Sanity tests for topic-dependent APIs — a topic (and optionally produced messages)
  must exist before these tests run.

  Covers:
  - CreateTopics V0-V5 (API key 19)
  - DeleteTopics V0-V4 (API key 20)
  - Produce V0-V8 (API key 0)
  - Fetch V0-V11 (API key 1)
  - ListOffsets V0-V5 (API key 2)
  - DeleteRecords V0-V1 (API key 21)
  - CreatePartitions V0-V1 (API key 37)
  - OffsetForLeaderEpoch V0-V3 (API key 23)
  - DescribeConfigs V0-V2 (API key 32)
  - AlterConfigs V0-V1 (API key 33)
  - IncrementalAlterConfigs V0-V1 (API key 44)

  Run with:
      mix test.sanity test/sanity/topic_sanity_test.exs
  """
  use Kayrock.SanityCase
  use ExUnit.Case, async: false

  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  # ---------------------------------------------------------------------------
  # Shared setup: one topic with 3 partitions; one produced message (offset 0)
  # ---------------------------------------------------------------------------

  setup_all %{kafka: kafka} do
    {:ok, client} = build_client(kafka)

    # Create the shared topic using a high API version (falls back to max supported)
    topic = create_topic(client, 5)
    :ok = wait_for_topic(client, topic)

    # Produce one message so Fetch / ListOffsets have data to read back
    record_set = Kayrock.RecordBatch.from_binary_list(["sanity-check"])

    produce_req =
      produce_messages_request(
        topic,
        [[partition: 0, record_set: record_set]],
        1,
        5
      )

    {:ok, _produce_resp} = Kayrock.client_call(client, produce_req, :random)

    %{client: client, topic: topic}
  end

  # ===========================================================================
  # CreateTopics V0-V5
  # ===========================================================================

  describe "CreateTopics" do
    # Each version test creates its OWN unique topic so the creation is the
    # test.  We use `unique_string/0` from TestSupport (imported via SanityCase).
    for version <- 0..5 do
      @tag api: :create_topics, version: version
      test "V#{version} creates a topic successfully", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        topic_name = unique_string()
        request = create_topic_request(topic_name, version)
        {:ok, response} = Kayrock.client_call(client, request, :controller)

        assert is_integer(response.correlation_id),
               "V#{version} correlation_id should be integer"

        assert is_list(response.topics),
               "V#{version} topics should be a list"

        topic_result = Enum.find(response.topics, fn t -> t.name == topic_name end)
        assert topic_result != nil, "V#{version} created topic should appear in response"

        assert topic_result.error_code == 0,
               "V#{version} topic creation error_code should be 0, got #{topic_result.error_code}"
      end
    end

    # V0 response has no throttle_time_ms
    @tag api: :create_topics, version: 0
    test "V0 response has no throttle_time_ms", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = create_topic_request(unique_string(), 0)
      {:ok, response} = Kayrock.client_call(client, request, :controller)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 CreateTopics response should not have throttle_time_ms"
    end

    # V2+ responses include throttle_time_ms
    for version <- 2..5 do
      @tag api: :create_topics, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = create_topic_request(unique_string(), version)
        {:ok, response} = Kayrock.client_call(client, request, :controller)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be >= 0"
      end
    end

    # V5 is flexible — response includes tagged_fields
    @tag api: :create_topics, version: 5
    test "V5 response includes tagged_fields (flexible version)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = create_topic_request(unique_string(), 5)
      {:ok, response} = Kayrock.client_call(client, request, :controller)

      assert is_list(response.tagged_fields),
             "V5 CreateTopics response.tagged_fields should be a list"
    end

    # Creating an already-existing topic should return error_code 36 (TOPIC_ALREADY_EXISTS)
    @tag api: :create_topics
    test "V3 creating duplicate topic returns TOPIC_ALREADY_EXISTS (36)", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)
      request = create_topic_request(topic, 3)
      {:ok, response} = Kayrock.client_call(client, request, :controller)

      topic_result = Enum.find(response.topics, fn t -> t.name == topic end)
      assert topic_result != nil

      # TOPIC_ALREADY_EXISTS = 36
      assert topic_result.error_code == 36,
             "Expected error_code 36 (TOPIC_ALREADY_EXISTS), got #{topic_result.error_code}"
    end
  end

  # ===========================================================================
  # DeleteTopics V0-V4
  # ===========================================================================

  describe "DeleteTopics" do
    # Each version creates a throwaway topic, then deletes it.
    for version <- 0..4 do
      @tag api: :delete_topics, version: version
      test "V#{version} deletes a topic successfully", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        # Create a throwaway topic (use high version; deletion version varies)
        throwaway = create_topic(client, 5)
        :ok = wait_for_topic(client, throwaway)

        # Build delete request — topic_names is a plain list of strings
        api_version = min(Kayrock.DeleteTopics.max_vsn(), version)
        request = Kayrock.DeleteTopics.get_request_struct(api_version)

        request =
          if api_version >= 4 do
            %{request | topic_names: [throwaway], timeout_ms: 5000, tagged_fields: []}
          else
            %{request | topic_names: [throwaway], timeout_ms: 5000}
          end

        {:ok, response} = Kayrock.client_call(client, request, :controller)

        assert is_integer(response.correlation_id),
               "V#{version} correlation_id should be integer"

        assert is_list(response.responses),
               "V#{version} responses should be a list"

        topic_result = Enum.find(response.responses, fn r -> r.name == throwaway end)
        assert topic_result != nil, "V#{version} deleted topic should appear in response"

        assert topic_result.error_code == 0,
               "V#{version} topic deletion error_code should be 0, got #{topic_result.error_code}"
      end
    end

    # V0 response has no throttle_time_ms
    @tag api: :delete_topics, version: 0
    test "V0 response has no throttle_time_ms", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      throwaway = create_topic(client, 5)
      :ok = wait_for_topic(client, throwaway)

      request = Kayrock.DeleteTopics.get_request_struct(0)
      request = %{request | topic_names: [throwaway], timeout_ms: 5000}
      {:ok, response} = Kayrock.client_call(client, request, :controller)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 DeleteTopics response should not have throttle_time_ms"
    end

    # V1+ responses include throttle_time_ms
    for version <- 1..4 do
      @tag api: :delete_topics, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        throwaway = create_topic(client, 5)
        :ok = wait_for_topic(client, throwaway)

        api_version = min(Kayrock.DeleteTopics.max_vsn(), version)
        request = Kayrock.DeleteTopics.get_request_struct(api_version)

        request =
          if api_version >= 4 do
            %{request | topic_names: [throwaway], timeout_ms: 5000, tagged_fields: []}
          else
            %{request | topic_names: [throwaway], timeout_ms: 5000}
          end

        {:ok, response} = Kayrock.client_call(client, request, :controller)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end

    # V4 is flexible — response includes tagged_fields
    @tag api: :delete_topics, version: 4
    test "V4 response includes tagged_fields (flexible version)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      throwaway = create_topic(client, 5)
      :ok = wait_for_topic(client, throwaway)

      request = %Kayrock.DeleteTopics.V4.Request{
        topic_names: [throwaway],
        timeout_ms: 5000,
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :controller)

      assert is_list(response.tagged_fields),
             "V4 DeleteTopics response.tagged_fields should be a list"
    end
  end

  # ===========================================================================
  # Produce V0-V8
  # ===========================================================================

  describe "Produce" do
    # V0-V2 use MessageSet format
    for version <- 0..2 do
      @tag api: :produce, version: version
      test "V#{version} produces a message (MessageSet format)", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        record_set = %Kayrock.MessageSet{
          messages: [
            %Kayrock.MessageSet.Message{key: nil, value: "msg-v#{version}", compression: :none}
          ]
        }

        request =
          produce_messages_request(topic, [[partition: 0, record_set: record_set]], 1, version)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_list(response.responses),
               "V#{version} responses should be a list"

        assert response.responses != [],
               "V#{version} responses should not be empty"

        [topic_response] = response.responses
        assert topic_response.topic == topic

        [partition_response] = topic_response.partition_responses

        assert partition_response.error_code == 0,
               "V#{version} partition error_code should be 0, got #{partition_response.error_code}"

        assert is_integer(partition_response.base_offset),
               "V#{version} base_offset should be integer"
      end
    end

    # V3+ use RecordBatch format
    for version <- 3..8 do
      @tag api: :produce, version: version
      test "V#{version} produces a message (RecordBatch format)", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        record_set = Kayrock.RecordBatch.from_binary_list(["msg-v#{version}"])

        request =
          produce_messages_request(topic, [[partition: 0, record_set: record_set]], 1, version)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_list(response.responses)
        assert response.responses != []

        [topic_response] = response.responses
        assert topic_response.topic == topic

        [partition_response] = topic_response.partition_responses

        assert partition_response.error_code == 0,
               "V#{version} partition error_code should be 0, got #{partition_response.error_code}"

        assert is_integer(partition_response.base_offset),
               "V#{version} base_offset should be integer"
      end
    end

    # V0 response has no throttle_time_ms
    @tag api: :produce, version: 0
    test "V0 response has no throttle_time_ms", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      record_set = %Kayrock.MessageSet{
        messages: [%Kayrock.MessageSet.Message{key: nil, value: "test", compression: :none}]
      }

      request = produce_messages_request(topic, [[partition: 0, record_set: record_set]], 1, 0)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 Produce response should not have throttle_time_ms"
    end

    # V1+ include throttle_time_ms
    for version <- 1..8 do
      @tag api: :produce, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        record_set =
          if version <= 2 do
            %Kayrock.MessageSet{
              messages: [
                %Kayrock.MessageSet.Message{key: nil, value: "t", compression: :none}
              ]
            }
          else
            Kayrock.RecordBatch.from_binary_list(["t"])
          end

        request =
          produce_messages_request(topic, [[partition: 0, record_set: record_set]], 1, version)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be >= 0"
      end
    end

    # V3+ include transactional_id field
    for version <- 3..8 do
      @tag api: :produce, version: version
      test "V#{version} request struct has transactional_id field", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        record_set = Kayrock.RecordBatch.from_binary_list(["tx-test"])

        request =
          produce_messages_request(topic, [[partition: 0, record_set: record_set]], 1, version)

        assert Map.has_key?(request, :transactional_id),
               "V#{version} request should have transactional_id"

        {:ok, response} = Kayrock.client_call(client, request, :random)
        assert is_list(response.responses)
      end
    end

    # V8 response includes record_errors and error_message (KIP-467)
    @tag api: :produce, version: 8
    test "V8 response partition entries include record_errors and error_message", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)
      record_set = Kayrock.RecordBatch.from_binary_list(["v8-msg"])
      request = produce_messages_request(topic, [[partition: 0, record_set: record_set]], 1, 8)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      [topic_response] = response.responses
      [partition_response] = topic_response.partition_responses

      assert Map.has_key?(partition_response, :record_errors),
             "V8 partition_response should have record_errors"

      assert is_list(partition_response.record_errors),
             "V8 partition_response.record_errors should be a list"

      assert Map.has_key?(partition_response, :error_message),
             "V8 partition_response should have error_message"
    end
  end

  # ===========================================================================
  # Fetch V0-V11
  # ===========================================================================

  describe "Fetch" do
    for version <- 0..11 do
      @tag api: :fetch, version: version
      test "V#{version} fetches from topic and returns parseable response", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          fetch_messages_request(
            [[topic: topic, partition: 0, fetch_offset: 0]],
            [max_wait_time: 500, min_bytes: 0],
            version
          )

        # V9+ requires current_leader_epoch in each partition sub-map
        request =
          if version >= 9 do
            updated_topics =
              Enum.map(request.topics, fn topic_entry ->
                updated_partitions =
                  Enum.map(topic_entry.partitions, fn p ->
                    Map.put_new(p, :current_leader_epoch, -1)
                  end)

                %{topic_entry | partitions: updated_partitions}
              end)

            %{request | topics: updated_topics}
          else
            request
          end

        # V11 requires rack_id (non-nullable string)
        request =
          if version >= 11, do: %{request | rack_id: ""}, else: request

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.correlation_id),
               "V#{version} correlation_id should be integer"

        assert is_list(response.responses),
               "V#{version} responses should be a list"
      end
    end

    # V0 has no throttle_time_ms
    @tag api: :fetch, version: 0
    test "V0 response has no throttle_time_ms", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request =
        fetch_messages_request(
          [[topic: topic, partition: 0, fetch_offset: 0]],
          [max_wait_time: 500, min_bytes: 0],
          0
        )

      {:ok, response} = Kayrock.client_call(client, request, :random)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 Fetch response should not have throttle_time_ms"
    end

    # V1+ include throttle_time_ms
    for version <- 1..11 do
      @tag api: :fetch, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          fetch_messages_request(
            [[topic: topic, partition: 0, fetch_offset: 0]],
            [max_wait_time: 500, min_bytes: 0],
            version
          )

        # V9+ requires current_leader_epoch in each partition sub-map
        request =
          if version >= 9 do
            updated_topics =
              Enum.map(request.topics, fn topic_entry ->
                updated_partitions =
                  Enum.map(topic_entry.partitions, fn p ->
                    Map.put_new(p, :current_leader_epoch, -1)
                  end)

                %{topic_entry | partitions: updated_partitions}
              end)

            %{request | topics: updated_topics}
          else
            request
          end

        # V11 requires rack_id (non-nullable string)
        request =
          if version >= 11, do: %{request | rack_id: ""}, else: request

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be >= 0"
      end
    end

    # Verify topic appears in response with error_code 0
    for version <- [0, 4, 7, 11] do
      @tag api: :fetch, version: version
      test "V#{version} response contains topic with error_code 0", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          fetch_messages_request(
            [[topic: topic, partition: 0, fetch_offset: 0]],
            [max_wait_time: 500, min_bytes: 0],
            version
          )

        # V9+ requires current_leader_epoch in each partition sub-map
        request =
          if version >= 9 do
            updated_topics =
              Enum.map(request.topics, fn topic_entry ->
                updated_partitions =
                  Enum.map(topic_entry.partitions, fn p ->
                    Map.put_new(p, :current_leader_epoch, -1)
                  end)

                %{topic_entry | partitions: updated_partitions}
              end)

            %{request | topics: updated_topics}
          else
            request
          end

        # V11 requires rack_id (non-nullable string)
        request =
          if version >= 11, do: %{request | rack_id: ""}, else: request

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert response.responses != [],
               "V#{version} fetch should return at least one topic response"

        [topic_resp | _] = response.responses
        [partition_resp | _] = topic_resp.partition_responses

        assert partition_resp.partition_header.error_code == 0,
               "V#{version} partition error_code should be 0, got #{partition_resp.partition_header.error_code}"
      end
    end

    # V7+ use session_id / session_epoch
    for version <- 7..11 do
      @tag api: :fetch, version: version
      test "V#{version} request struct has session_id and session_epoch fields", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          fetch_messages_request(
            [[topic: topic, partition: 0, fetch_offset: 0]],
            [max_wait_time: 500, min_bytes: 0],
            version
          )

        # V9+ requires current_leader_epoch in each partition sub-map
        request =
          if version >= 9 do
            updated_topics =
              Enum.map(request.topics, fn topic_entry ->
                updated_partitions =
                  Enum.map(topic_entry.partitions, fn p ->
                    Map.put_new(p, :current_leader_epoch, -1)
                  end)

                %{topic_entry | partitions: updated_partitions}
              end)

            %{request | topics: updated_topics}
          else
            request
          end

        # V11 requires rack_id (non-nullable string)
        request =
          if version >= 11, do: %{request | rack_id: ""}, else: request

        assert Map.has_key?(request, :session_id),
               "V#{version} request should have session_id"

        assert Map.has_key?(request, :session_epoch),
               "V#{version} request should have session_epoch"

        {:ok, response} = Kayrock.client_call(client, request, :random)
        assert is_list(response.responses)
      end
    end
  end

  # ===========================================================================
  # ListOffsets V0-V5
  # ===========================================================================

  describe "ListOffsets" do
    for version <- 0..5 do
      @tag api: :list_offsets, version: version
      test "V#{version} returns parseable response", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          list_offsets_request(
            topic,
            [[partition: 0, timestamp: -1]],
            version
          )

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.correlation_id),
               "V#{version} correlation_id should be integer"

        assert is_list(response.responses),
               "V#{version} responses should be a list"

        assert response.responses != [],
               "V#{version} responses should not be empty"
      end
    end

    # V0 response has no throttle_time_ms
    @tag api: :list_offsets, version: 0
    test "V0 response has no throttle_time_ms", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)
      request = list_offsets_request(topic, [[partition: 0, timestamp: -1]], 0)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 ListOffsets response should not have throttle_time_ms"
    end

    # V2+ include throttle_time_ms
    for version <- 2..5 do
      @tag api: :list_offsets, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = list_offsets_request(topic, [[partition: 0, timestamp: -1]], version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be >= 0"
      end
    end

    # Verify partition response structure
    for version <- [0, 1, 5] do
      @tag api: :list_offsets, version: version
      test "V#{version} partition response has offset and error_code", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = list_offsets_request(topic, [[partition: 0, timestamp: -1]], version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        [topic_response] = response.responses
        [partition_response] = topic_response.partition_responses

        assert is_integer(partition_response.error_code),
               "V#{version} partition error_code should be integer"

        assert partition_response.error_code == 0,
               "V#{version} partition error_code should be 0, got #{partition_response.error_code}"
      end
    end

    # V0 uses max_num_offsets (old format); V1+ returns single offset
    @tag api: :list_offsets, version: 0
    test "V0 partition response has offsets list (old format)", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)
      request = list_offsets_request(topic, [[partition: 0, timestamp: -2]], 0)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      [topic_response] = response.responses
      [partition_response] = topic_response.partition_responses

      assert is_list(partition_response.offsets),
             "V0 ListOffsets partition_response should have offsets list"
    end

    # V1+ response returns a single offset (int64)
    @tag api: :list_offsets, version: 1
    test "V1 partition response has timestamp and offset fields", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)
      request = list_offsets_request(topic, [[partition: 0, timestamp: -1]], 1)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      [topic_response] = response.responses
      [partition_response] = topic_response.partition_responses

      assert Map.has_key?(partition_response, :timestamp),
             "V1 partition_response should have timestamp"

      assert Map.has_key?(partition_response, :offset),
             "V1 partition_response should have offset"
    end
  end

  # ===========================================================================
  # DeleteRecords V0-V1
  # ===========================================================================

  describe "DeleteRecords" do
    for version <- 0..1 do
      @tag api: :delete_records, version: version
      test "V#{version} deletes records up to offset 0 and returns success", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.DeleteRecords.max_vsn(), version)
        request = Kayrock.DeleteRecords.get_request_struct(api_version)

        request = %{
          request
          | topics: [
              %{
                topic: topic,
                partitions: [%{partition: 0, offset: 0}]
              }
            ],
            timeout: 5000
        }

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.topics),
               "V#{version} topics should be a list"

        topic_resp = Enum.find(response.topics, fn t -> t.topic == topic end)
        assert topic_resp != nil, "V#{version} topic should appear in response"

        [partition_resp] = topic_resp.partitions

        assert partition_resp.error_code == 0,
               "V#{version} partition error_code should be 0, got #{partition_resp.error_code}"
      end
    end

    # Verify throttle_time_ms in both versions
    for version <- 0..1 do
      @tag api: :delete_records, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.DeleteRecords.max_vsn(), version)
        request = Kayrock.DeleteRecords.get_request_struct(api_version)

        request = %{
          request
          | topics: [
              %{
                topic: topic,
                partitions: [%{partition: 0, offset: 0}]
              }
            ],
            timeout: 5000
        }

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0
      end
    end
  end

  # ===========================================================================
  # CreatePartitions V0-V1
  # ===========================================================================

  describe "CreatePartitions" do
    # Create a fresh topic for each partition-increase test to avoid conflicts
    for version <- 0..1 do
      @tag api: :create_partitions, version: version
      test "V#{version} increases partition count and returns success", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        # Create a single-partition topic for this test
        topic_name = unique_string()
        # Create with 1 partition by using custom create request
        create_req = Kayrock.CreateTopics.get_request_struct(3)

        create_req = %{
          create_req
          | topics: [
              %{
                name: topic_name,
                num_partitions: 1,
                replication_factor: 1,
                assignments: [],
                configs: []
              }
            ],
            timeout_ms: 5000,
            validate_only: false
        }

        {:ok, _} = Kayrock.client_call(client, create_req, :controller)
        :ok = wait_for_topic(client, topic_name)

        # Now increase to 3 partitions
        api_version = min(Kayrock.CreatePartitions.max_vsn(), version)
        request = Kayrock.CreatePartitions.get_request_struct(api_version)

        request = %{
          request
          | topic_partitions: [
              %{
                topic: topic_name,
                new_partitions: %{count: 3, assignment: nil}
              }
            ],
            timeout: 5000,
            validate_only: false
        }

        {:ok, response} = Kayrock.client_call(client, request, :controller)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.topic_errors),
               "V#{version} topic_errors should be a list"

        topic_result = Enum.find(response.topic_errors, fn t -> t.topic == topic_name end)
        assert topic_result != nil

        assert topic_result.error_code == 0,
               "V#{version} topic_errors error_code should be 0, got #{topic_result.error_code}"
      end
    end

    # Verify response structure has topic_errors (not results)
    @tag api: :create_partitions, version: 0
    test "V0 response has topic_errors field (not results)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      topic_name = unique_string()

      create_req = Kayrock.CreateTopics.get_request_struct(3)

      create_req = %{
        create_req
        | topics: [
            %{
              name: topic_name,
              num_partitions: 1,
              replication_factor: 1,
              assignments: [],
              configs: []
            }
          ],
          timeout_ms: 5000,
          validate_only: false
      }

      {:ok, _} = Kayrock.client_call(client, create_req, :controller)
      :ok = wait_for_topic(client, topic_name)

      request = %Kayrock.CreatePartitions.V0.Request{
        topic_partitions: [%{topic: topic_name, new_partitions: %{count: 3, assignment: nil}}],
        timeout: 5000,
        validate_only: false
      }

      {:ok, response} = Kayrock.client_call(client, request, :controller)

      assert Map.has_key?(response, :topic_errors),
             "V0 CreatePartitions response should have topic_errors field"

      refute Map.has_key?(response, :results),
             "V0 CreatePartitions response should NOT have results field"
    end
  end

  # ===========================================================================
  # OffsetForLeaderEpoch V0-V3
  # ===========================================================================

  describe "OffsetForLeaderEpoch" do
    # V0/V1: topics + partitions with leader_epoch only
    for version <- 0..1 do
      @tag api: :offset_for_leader_epoch, version: version
      test "V#{version} returns parseable response", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.OffsetForLeaderEpoch.max_vsn(), version)
        request = Kayrock.OffsetForLeaderEpoch.get_request_struct(api_version)

        request = %{
          request
          | topics: [
              %{
                topic: topic,
                partitions: [%{partition: 0, leader_epoch: 0}]
              }
            ]
        }

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.correlation_id),
               "V#{version} correlation_id should be integer"

        assert is_list(response.topics),
               "V#{version} topics should be a list"
      end
    end

    # V2: topics + partitions with current_leader_epoch + leader_epoch (no replica_id)
    @tag api: :offset_for_leader_epoch, version: 2
    test "V2 returns parseable response", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = Kayrock.OffsetForLeaderEpoch.get_request_struct(2)

      request = %{
        request
        | topics: [
            %{
              topic: topic,
              partitions: [%{partition: 0, current_leader_epoch: -1, leader_epoch: 0}]
            }
          ]
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.correlation_id)
      assert is_list(response.topics)
    end

    # V3: adds replica_id field
    @tag api: :offset_for_leader_epoch, version: 3
    test "V3 request has replica_id field and returns parseable response", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)

      request = Kayrock.OffsetForLeaderEpoch.get_request_struct(3)

      request = %{
        request
        | replica_id: -1,
          topics: [
            %{
              topic: topic,
              partitions: [%{partition: 0, current_leader_epoch: -1, leader_epoch: 0}]
            }
          ]
      }

      assert Map.has_key?(request, :replica_id),
             "V3 OffsetForLeaderEpoch request should have replica_id"

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.correlation_id)
      assert is_list(response.topics)
    end

    # V0 response has no throttle_time_ms
    @tag api: :offset_for_leader_epoch, version: 0
    test "V0 response has no throttle_time_ms", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.OffsetForLeaderEpoch.V0.Request{
        topics: [
          %{topic: topic, partitions: [%{partition: 0, leader_epoch: 0}]}
        ]
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 OffsetForLeaderEpoch response should not have throttle_time_ms"
    end

    # V2+ include throttle_time_ms
    for version <- 2..3 do
      @tag api: :offset_for_leader_epoch, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.OffsetForLeaderEpoch.max_vsn(), version)
        request = Kayrock.OffsetForLeaderEpoch.get_request_struct(api_version)

        request = %{
          request
          | topics: [
              %{
                topic: topic,
                partitions: [%{partition: 0, current_leader_epoch: -1, leader_epoch: 0}]
              }
            ]
        }

        request =
          if api_version >= 3 do
            %{request | replica_id: -1}
          else
            request
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end
  end

  # ===========================================================================
  # DescribeConfigs V0-V2
  # ===========================================================================

  describe "DescribeConfigs" do
    # resource_type 2 = TOPIC
    for version <- 0..2 do
      @tag api: :describe_configs, version: version
      test "V#{version} describes topic config and returns parseable response", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.DescribeConfigs.max_vsn(), version)
        request = Kayrock.DescribeConfigs.get_request_struct(api_version)

        resource = %{
          resource_type: 2,
          resource_name: topic,
          config_names: []
        }

        request =
          if api_version >= 1 do
            %{request | resources: [resource], include_synonyms: false}
          else
            %{request | resources: [resource]}
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.resources),
               "V#{version} resources should be a list"

        assert response.resources != [],
               "V#{version} resources should not be empty"
      end
    end

    # Verify resource entry structure
    for version <- [0, 2] do
      @tag api: :describe_configs, version: version
      test "V#{version} resource entry has error_code and configs", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.DescribeConfigs.max_vsn(), version)
        request = Kayrock.DescribeConfigs.get_request_struct(api_version)

        resource = %{resource_type: 2, resource_name: topic, config_names: []}

        request =
          if api_version >= 1 do
            %{request | resources: [resource], include_synonyms: false}
          else
            %{request | resources: [resource]}
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        [resource_result] = response.resources

        assert is_integer(resource_result.error_code),
               "V#{version} resource_result.error_code should be integer"

        assert resource_result.error_code == 0,
               "V#{version} resource error_code should be 0, got #{resource_result.error_code}"

        assert is_list(resource_result.config_entries),
               "V#{version} resource_result.config_entries should be a list"
      end
    end

    # V1+ supports include_synonyms
    @tag api: :describe_configs, version: 1
    test "V1 include_synonyms field accepted without error", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.DescribeConfigs.V1.Request{
        resources: [%{resource_type: 2, resource_name: topic, config_names: []}],
        include_synonyms: true
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.resources)
    end
  end

  # ===========================================================================
  # AlterConfigs V0-V1
  # ===========================================================================

  describe "AlterConfigs" do
    # resource_type 2 = TOPIC; set retention.ms to a valid value
    for version <- 0..1 do
      @tag api: :alter_configs, version: version
      test "V#{version} alters topic config and returns success", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.AlterConfigs.max_vsn(), version)
        request = Kayrock.AlterConfigs.get_request_struct(api_version)

        request = %{
          request
          | resources: [
              %{
                resource_type: 2,
                resource_name: topic,
                config_entries: [%{config_name: "retention.ms", config_value: "604800000"}]
              }
            ],
            validate_only: false
        }

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.resources),
               "V#{version} resources should be a list"
      end
    end

    # Verify resource entry has error_code 0
    for version <- 0..1 do
      @tag api: :alter_configs, version: version
      test "V#{version} resource entry has error_code 0", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.AlterConfigs.max_vsn(), version)
        request = Kayrock.AlterConfigs.get_request_struct(api_version)

        request = %{
          request
          | resources: [
              %{
                resource_type: 2,
                resource_name: topic,
                config_entries: [%{config_name: "retention.ms", config_value: "604800001"}]
              }
            ],
            validate_only: false
        }

        {:ok, response} = Kayrock.client_call(client, request, :random)

        [resource_result] = response.resources

        assert is_integer(resource_result.error_code),
               "V#{version} resource_result.error_code should be integer"

        assert resource_result.error_code == 0,
               "V#{version} resource error_code should be 0, got #{resource_result.error_code}"
      end
    end
  end

  # ===========================================================================
  # IncrementalAlterConfigs V0-V1
  # ===========================================================================

  describe "IncrementalAlterConfigs" do
    # config_operation 0 = SET
    for version <- 0..1 do
      @tag api: :incremental_alter_configs, version: version
      test "V#{version} incrementally alters topic config and returns success", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.IncrementalAlterConfigs.max_vsn(), version)
        request = Kayrock.IncrementalAlterConfigs.get_request_struct(api_version)

        resource = %{
          resource_type: 2,
          resource_name: topic,
          configs: [%{name: "retention.ms", config_operation: 0, value: "86400000"}]
        }

        request =
          if api_version >= 1 do
            %{
              request
              | resources: [Map.put(resource, :tagged_fields, [])],
                validate_only: false,
                tagged_fields: []
            }
          else
            %{request | resources: [resource], validate_only: false}
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.responses),
               "V#{version} responses should be a list"
      end
    end

    # Verify response has responses field (not resources)
    @tag api: :incremental_alter_configs, version: 0
    test "V0 response has responses field (not resources)", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.IncrementalAlterConfigs.V0.Request{
        resources: [
          %{
            resource_type: 2,
            resource_name: topic,
            configs: [%{name: "retention.ms", config_operation: 0, value: "86400001"}]
          }
        ],
        validate_only: false
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert Map.has_key?(response, :responses),
             "V0 IncrementalAlterConfigs response should have responses field"

      refute Map.has_key?(response, :resources),
             "V0 IncrementalAlterConfigs response should NOT have resources field"
    end

    # V1 is flexible — response includes tagged_fields
    @tag api: :incremental_alter_configs, version: 1
    test "V1 response includes tagged_fields (flexible version)", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.IncrementalAlterConfigs.V1.Request{
        resources: [
          %{
            resource_type: 2,
            resource_name: topic,
            configs: [
              %{
                name: "retention.ms",
                config_operation: 0,
                value: "86400002",
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ],
        validate_only: false,
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.tagged_fields),
             "V1 IncrementalAlterConfigs response.tagged_fields should be a list"
    end

    # Error case: response entry error_code should be 0 on success
    for version <- 0..1 do
      @tag api: :incremental_alter_configs, version: version
      test "V#{version} response entry has error_code 0", %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        api_version = min(Kayrock.IncrementalAlterConfigs.max_vsn(), version)
        request = Kayrock.IncrementalAlterConfigs.get_request_struct(api_version)

        resource = %{
          resource_type: 2,
          resource_name: topic,
          configs: [%{name: "retention.ms", config_operation: 0, value: "86400003"}]
        }

        request =
          if api_version >= 1 do
            %{
              request
              | resources: [Map.put(resource, :tagged_fields, [])],
                validate_only: false,
                tagged_fields: []
            }
          else
            %{request | resources: [resource], validate_only: false}
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        [result] = response.responses

        assert is_integer(result.error_code),
               "V#{version} response entry error_code should be integer"

        assert result.error_code == 0,
               "V#{version} response entry error_code should be 0, got #{result.error_code}"
      end
    end
  end
end
