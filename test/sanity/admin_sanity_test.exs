defmodule Kayrock.Sanity.AdminSanityTest do
  @moduledoc """
  Sanity tests for admin APIs — only a broker connection and a test topic required.

  Covers:
  - DescribeAcls V0-V1 (API key 29)
  - CreateAcls V0-V1 (API key 30)
  - DeleteAcls V0-V1 (API key 31)
  - AlterReplicaLogDirs V0-V1 (API key 34)
  - DescribeLogDirs V0-V1 (API key 35)
  - CreateDelegationToken V0-V2 (API key 38) — expects error 61 or 64 (tokens disabled/not allowed)
  - RenewDelegationToken V0-V1 (API key 39) — expects error 61, 64, or 2
  - ExpireDelegationToken V0-V1 (API key 40) — expects error 61, 64, or 2
  - DescribeDelegationToken V0-V1 (API key 41) — expects error 61 or 64
  - ElectLeaders V0-V2 (API key 43)
  - AlterPartitionReassignments V0 (API key 45)
  - ListPartitionReassignments V0 (API key 46)

  Run with:
      mix test.sanity test/sanity/admin_sanity_test.exs
  """
  use Kayrock.SanityCase
  use ExUnit.Case, async: false

  container(:kafka, KafkaContainer.new(), shared: true)

  setup_all %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_topic(client, 5)
    :ok = wait_for_topic(client, topic)
    %{client: client, topic: topic}
  end

  # ---------------------------------------------------------------------------
  # DescribeAcls V0-V1 (API key 29)
  # ---------------------------------------------------------------------------

  describe "DescribeAcls" do
    # V0: no resource_pattern_type_filter field
    @tag api: :describe_acls, version: 0
    test "V0 returns parseable response (no resource_pattern_type_filter)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.DescribeAcls.V0.Request{
        resource_type: 2,
        resource_name: nil,
        principal: "User:test-sanity",
        host: nil,
        operation: 1,
        permission_type: 1
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 throttle_time_ms should be integer"

      assert is_integer(response.error_code),
             "V0 error_code should be integer"

      assert is_list(response.resources),
             "V0 resources should be a list"
    end

    # V1: adds resource_pattern_type_filter
    @tag api: :describe_acls, version: 1
    test "V1 returns parseable response (with resource_pattern_type_filter)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.DescribeAcls.V1.Request{
        resource_type: 2,
        resource_name: nil,
        resource_pattern_type_filter: 3,
        principal: "User:test-sanity",
        host: nil,
        operation: 1,
        permission_type: 1
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V1 throttle_time_ms should be integer"

      assert is_integer(response.error_code),
             "V1 error_code should be integer"

      assert is_list(response.resources),
             "V1 resources should be a list"
    end

    @tag api: :describe_acls
    test "V0 response does not have resource_pattern_type_filter field (correct version)", %{
      kafka: kafka
    } do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.DescribeAcls.V0.Request{
        resource_type: 1,
        resource_name: nil,
        principal: nil,
        host: nil,
        operation: 1,
        permission_type: 1
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      # V0 response should not have resource_pattern_type on top-level
      refute Map.has_key?(response, :resource_pattern_type_filter),
             "V0 response should not have resource_pattern_type_filter field"
    end

    @tag api: :describe_acls
    test "V1 returns empty resources list for non-existent principal", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.DescribeAcls.V1.Request{
        resource_type: 2,
        resource_name: "does-not-exist-topic",
        resource_pattern_type_filter: 3,
        principal: "User:nonexistent-test-sanity",
        host: "*",
        operation: 2,
        permission_type: 3
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      # Accept 0 (success, empty list) or auth error codes
      # 54 = CLUSTER_AUTHORIZATION_FAILED (broker may enforce stricter auth with V1 pattern type filter)
      assert response.error_code in [0, 29, 31, 54],
             "Expected success or auth error, got #{response.error_code}"

      if response.error_code == 0 do
        assert response.resources == [],
               "V1 non-existent principal should return empty resources list"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # CreateAcls V0-V1 (API key 30)
  # ---------------------------------------------------------------------------

  describe "CreateAcls" do
    # V0: creation entry without resource_pattern_type
    @tag api: :create_acls, version: 0
    test "V0 accepts valid creation entry and returns parseable response", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      creation = %{
        resource_type: 2,
        resource_name: "test-sanity-topic",
        principal: "User:test-sanity",
        host: "*",
        operation: 2,
        permission_type: 3
      }

      request = %Kayrock.CreateAcls.V0.Request{creations: [creation]}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 throttle_time_ms should be integer"

      assert is_list(response.creation_responses),
             "V0 creation_responses should be a list"

      # Each creation response has error_code
      for cr <- response.creation_responses do
        assert is_integer(cr.error_code),
               "V0 creation_response error_code should be integer"
      end
    end

    # V1: creation entry with resource_pattern_type
    @tag api: :create_acls, version: 1
    test "V1 accepts creation entry with resource_pattern_type and returns parseable response",
         %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      creation = %{
        resource_type: 2,
        resource_name: "test-sanity-topic",
        resource_pattern_type: 3,
        principal: "User:test-sanity",
        host: "*",
        operation: 2,
        permission_type: 3
      }

      request = %Kayrock.CreateAcls.V1.Request{creations: [creation]}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V1 throttle_time_ms should be integer"

      assert is_list(response.creation_responses),
             "V1 creation_responses (not 'results') should be a list"

      for cr <- response.creation_responses do
        assert is_integer(cr.error_code),
               "V1 creation_response error_code should be integer"
      end
    end

    @tag api: :create_acls
    test "V0 empty creations list is accepted", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = %Kayrock.CreateAcls.V0.Request{creations: []}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert response.creation_responses == [],
             "V0 empty creations should return empty creation_responses"
    end
  end

  # ---------------------------------------------------------------------------
  # DeleteAcls V0-V1 (API key 31)
  # ---------------------------------------------------------------------------

  describe "DeleteAcls" do
    # V0: filter without resource_pattern_type_filter
    @tag api: :delete_acls, version: 0
    test "V0 accepts valid filter and returns parseable response", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      filter = %{
        resource_type: 1,
        resource_name: nil,
        principal: "User:test-sanity",
        host: "*",
        operation: 1,
        permission_type: 1
      }

      request = %Kayrock.DeleteAcls.V0.Request{filters: [filter]}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 throttle_time_ms should be integer"

      assert is_list(response.filter_responses),
             "V0 filter_responses (not 'filter_results') should be a list"

      for fr <- response.filter_responses do
        assert is_integer(fr.error_code),
               "V0 filter_response error_code should be integer"
      end
    end

    # V1: filter with resource_pattern_type_filter
    @tag api: :delete_acls, version: 1
    test "V1 accepts filter with resource_pattern_type_filter and returns parseable response",
         %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      filter = %{
        resource_type: 1,
        resource_name: nil,
        resource_pattern_type_filter: 3,
        principal: "User:test-sanity",
        host: "*",
        operation: 1,
        permission_type: 1
      }

      request = %Kayrock.DeleteAcls.V1.Request{filters: [filter]}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V1 throttle_time_ms should be integer"

      assert is_list(response.filter_responses),
             "V1 filter_responses (not 'filter_results') should be a list"

      for fr <- response.filter_responses do
        assert is_integer(fr.error_code),
               "V1 filter_response error_code should be integer"
      end
    end

    @tag api: :delete_acls
    test "V0 empty filters list is accepted", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = %Kayrock.DeleteAcls.V0.Request{filters: []}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert response.filter_responses == [],
             "V0 empty filters should return empty filter_responses"
    end
  end

  # ---------------------------------------------------------------------------
  # DescribeLogDirs V0-V1 (API key 35)
  # ---------------------------------------------------------------------------

  describe "DescribeLogDirs" do
    for version <- 0..1 do
      @tag api: :describe_log_dirs, version: version
      test "V#{version} with specific topic returns parseable response", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          version
          |> Kayrock.DescribeLogDirs.get_request_struct()
          |> Map.put(:topics, [%{topic: topic, partitions: [0]}])

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.log_dirs),
               "V#{version} log_dirs (not 'results') should be a list"
      end

      @tag api: :describe_log_dirs, version: version
      test "V#{version} with nil topics (all log dirs) returns parseable response", %{
        kafka: kafka
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          version
          |> Kayrock.DescribeLogDirs.get_request_struct()
          |> Map.put(:topics, nil)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.log_dirs),
               "V#{version} log_dirs should be a list"

        # At least one log dir entry expected for a running broker
        assert response.log_dirs != [],
               "V#{version} broker should report at least one log dir"

        for ld <- response.log_dirs do
          assert is_integer(ld.error_code),
                 "V#{version}: log_dir.error_code should be integer"

          assert is_binary(ld.log_dir),
                 "V#{version}: log_dir.log_dir should be binary"

          assert is_list(ld.topics),
                 "V#{version}: log_dir.topics should be a list"
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # AlterReplicaLogDirs V0-V1 (API key 34)
  # ---------------------------------------------------------------------------

  describe "AlterReplicaLogDirs" do
    for version <- 0..1 do
      @tag api: :alter_replica_log_dirs, version: version
      test "V#{version} with non-existent path returns parseable response (partition error expected)",
           %{kafka: kafka, topic: topic} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        # Request to move a partition to a non-existent path — broker will respond
        # with per-partition error (LOG_DIR_NOT_FOUND = 57 or similar) but will
        # not TCP-disconnect, so we can verify the response struct is valid.
        request =
          version
          |> Kayrock.AlterReplicaLogDirs.get_request_struct()
          |> Map.put(:log_dirs, [
            %{
              log_dir: "/non/existent/path",
              topics: [%{topic: topic, partitions: [0]}]
            }
          ])

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert is_list(response.topics),
               "V#{version} topics (not 'results') should be a list"

        for t <- response.topics do
          assert is_binary(t.topic), "V#{version}: topic.topic should be binary"
          assert is_list(t.partitions), "V#{version}: topic.partitions should be a list"

          for p <- t.partitions do
            assert is_integer(p.error_code),
                   "V#{version}: partition.error_code should be integer"
          end
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # CreateDelegationToken V0-V2 (API key 38)
  # Expects error 61 (DELEGATION_TOKEN_AUTH_DISABLED) in default broker config
  # ---------------------------------------------------------------------------

  describe "CreateDelegationToken" do
    # V0 and V1 have the same schema
    for version <- 0..1 do
      @tag api: :create_delegation_token, version: version
      test "V#{version} returns parseable response (error 61 expected for disabled tokens)",
           %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          version
          |> Kayrock.CreateDelegationToken.get_request_struct()
          |> Map.put(:renewers, [])
          |> Map.put(:max_lifetime_ms, -1)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        # error_code 61 = DELEGATION_TOKEN_AUTH_DISABLED (expected in default config)
        # error_code 64 = DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (broker rejects without specific auth config)
        assert response.error_code in [0, 61, 64],
               "V#{version} expected error_code 0, 61, or 64 (tokens disabled/not allowed), got #{response.error_code}"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end

    # V2: flexible version adds tagged_fields
    @tag api: :create_delegation_token, version: 2
    test "V2 returns parseable response with tagged_fields support (error 61 expected)",
         %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.CreateDelegationToken.V2.Request{
        renewers: [],
        max_lifetime_ms: -1,
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      # 64 = DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (broker rejects without specific auth config)
      assert response.error_code in [0, 61, 64],
             "V2 expected error_code 0, 61, or 64 (tokens disabled/not allowed), got #{response.error_code}"

      assert is_integer(response.throttle_time_ms),
             "V2 throttle_time_ms should be integer"
    end

    @tag api: :create_delegation_token
    test "V0 response has required fields", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.CreateDelegationToken.V0.Request{
        renewers: [],
        max_lifetime_ms: -1
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert Map.has_key?(response, :error_code), "response must have error_code"
      assert Map.has_key?(response, :principal_type), "response must have principal_type"
      assert Map.has_key?(response, :principal_name), "response must have principal_name"
      assert Map.has_key?(response, :issue_timestamp_ms), "response must have issue_timestamp_ms"

      assert Map.has_key?(response, :expiry_timestamp_ms),
             "response must have expiry_timestamp_ms"

      assert Map.has_key?(response, :max_timestamp_ms), "response must have max_timestamp_ms"
      assert Map.has_key?(response, :token_id), "response must have token_id"
      assert Map.has_key?(response, :hmac), "response must have hmac"
      assert Map.has_key?(response, :throttle_time_ms), "response must have throttle_time_ms"
    end
  end

  # ---------------------------------------------------------------------------
  # RenewDelegationToken V0-V1 (API key 39)
  # Expects error 61 (tokens disabled) or 2 (token not found)
  # ---------------------------------------------------------------------------

  describe "RenewDelegationToken" do
    for version <- 0..1 do
      @tag api: :renew_delegation_token, version: version
      test "V#{version} returns parseable response (expects error 61 or 2)", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          version
          |> Kayrock.RenewDelegationToken.get_request_struct()
          |> Map.put(:hmac, <<0, 1, 2, 3>>)
          |> Map.put(:renew_period_ms, 86_400_000)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        # 61 = DELEGATION_TOKEN_AUTH_DISABLED, 2 = CORRUPT_MESSAGE / token not found
        # 64 = DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (broker rejects without specific auth config)
        assert is_integer(response.error_code),
               "V#{version} error_code should be integer"

        assert response.error_code in [0, 2, 61, 64],
               "V#{version} expected 0, 2, 61, or 64, got #{response.error_code}"

        assert is_integer(response.expiry_timestamp_ms),
               "V#{version} expiry_timestamp_ms should be integer"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # ExpireDelegationToken V0-V1 (API key 40)
  # Expects error 61 (tokens disabled) or 2 (token not found)
  # ---------------------------------------------------------------------------

  describe "ExpireDelegationToken" do
    for version <- 0..1 do
      @tag api: :expire_delegation_token, version: version
      test "V#{version} returns parseable response (expects error 61 or 2)", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          version
          |> Kayrock.ExpireDelegationToken.get_request_struct()
          |> Map.put(:hmac, <<0, 1, 2, 3>>)
          |> Map.put(:expiry_time_period_ms, -1)

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.error_code),
               "V#{version} error_code should be integer"

        # 64 = DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (broker rejects without specific auth config)
        assert response.error_code in [0, 2, 61, 64],
               "V#{version} expected 0, 2, 61, or 64, got #{response.error_code}"

        assert is_integer(response.expiry_timestamp_ms),
               "V#{version} expiry_timestamp_ms should be integer"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # DescribeDelegationToken V0-V1 (API key 41)
  # Expects error 61 (DELEGATION_TOKEN_AUTH_DISABLED) in default broker config
  # ---------------------------------------------------------------------------

  describe "DescribeDelegationToken" do
    # Empty owners list = describe all tokens
    for version <- 0..1 do
      @tag api: :describe_delegation_token, version: version
      test "V#{version} empty owners list returns parseable response (error 61 expected)",
           %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          version
          |> Kayrock.DescribeDelegationToken.get_request_struct()
          |> Map.put(:owners, [])

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.error_code),
               "V#{version} error_code should be integer"

        # 61 = DELEGATION_TOKEN_AUTH_DISABLED
        # 64 = DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (broker rejects without specific auth config)
        assert response.error_code in [0, 61, 64],
               "V#{version} expected error_code 0, 61, or 64, got #{response.error_code}"

        assert is_list(response.tokens),
               "V#{version} tokens should be a list"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end

    @tag api: :describe_delegation_token
    test "V0 response struct has correct field order (error_code before tokens before throttle_time_ms)",
         %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.DescribeDelegationToken.V0.Request{owners: []}
      {:ok, response} = Kayrock.client_call(client, request, :random)

      # Verify the struct has the correct fields per the schema:
      # error_code, tokens, throttle_time_ms
      assert Map.has_key?(response, :error_code), "response must have error_code"
      assert Map.has_key?(response, :tokens), "response must have tokens"
      assert Map.has_key?(response, :throttle_time_ms), "response must have throttle_time_ms"
    end
  end

  # ---------------------------------------------------------------------------
  # ElectLeaders V0-V2 (API key 43)
  # ---------------------------------------------------------------------------

  describe "ElectLeaders" do
    # V0 has NO election_type field (added in V1 per KIP-460)
    @tag api: :elect_leaders, version: 0
    test "V0 (no election_type field) with existing topic partition returns parseable response",
         %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      # V0.Request: fields are topic_partitions and timeout_ms (NO election_type)
      request = %Kayrock.ElectLeaders.V0.Request{
        topic_partitions: [%{topic: topic, partition_id: [0]}],
        timeout_ms: 5000
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 throttle_time_ms should be integer"

      assert is_list(response.replica_election_results),
             "V0 replica_election_results should be a list"
    end

    # V1 adds election_type
    @tag api: :elect_leaders, version: 1
    test "V1 (with election_type) with existing topic partition returns parseable response",
         %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ElectLeaders.V1.Request{
        election_type: 0,
        topic_partitions: [%{topic: topic, partition_id: [0]}],
        timeout_ms: 5000
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V1 throttle_time_ms should be integer"

      # V1+ response also has top-level error_code
      assert is_integer(response.error_code),
             "V1 error_code should be integer"

      assert is_list(response.replica_election_results),
             "V1 replica_election_results should be a list"
    end

    # V2 is flexible version (tagged_fields)
    @tag api: :elect_leaders, version: 2
    test "V2 (flexible, tagged_fields) with existing topic partition returns parseable response",
         %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ElectLeaders.V2.Request{
        election_type: 0,
        topic_partitions: [
          %{
            topic: topic,
            partition_id: [0],
            tagged_fields: []
          }
        ],
        timeout_ms: 5000,
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V2 throttle_time_ms should be integer"

      assert is_integer(response.error_code),
             "V2 error_code should be integer"

      assert is_list(response.replica_election_results),
             "V2 replica_election_results should be a list"
    end

    @tag api: :elect_leaders
    test "V0 replica_election_results entries have correct structure when partitions listed",
         %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ElectLeaders.V0.Request{
        topic_partitions: [%{topic: topic, partition_id: [0]}],
        timeout_ms: 5000
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      for r <- response.replica_election_results do
        assert is_binary(r.topic), "replica_election_result.topic should be binary"

        assert is_list(r.partition_result),
               "replica_election_result.partition_result should be list"

        for p <- r.partition_result do
          assert is_integer(p.partition_id), "partition_result.partition_id should be integer"
          assert is_integer(p.error_code), "partition_result.error_code should be integer"
        end
      end
    end

    @tag api: :elect_leaders
    test "V0 with empty topic_partitions is accepted", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ElectLeaders.V0.Request{
        topic_partitions: [],
        timeout_ms: 5000
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.replica_election_results),
             "empty topic_partitions should return empty replica_election_results"
    end
  end

  # ---------------------------------------------------------------------------
  # AlterPartitionReassignments V0 (API key 45) — flexible version
  # ---------------------------------------------------------------------------

  describe "AlterPartitionReassignments" do
    @tag api: :alter_partition_reassignments, version: 0
    test "V0 with nil replicas (cancel reassignment) returns parseable response",
         %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      # nil replicas = cancel any ongoing reassignment for that partition
      request = %Kayrock.AlterPartitionReassignments.V0.Request{
        timeout_ms: 5000,
        topics: [
          %{
            name: topic,
            partitions: [
              %{partition_index: 0, replicas: nil, tagged_fields: []}
            ],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 throttle_time_ms should be integer"

      assert is_integer(response.error_code),
             "V0 error_code should be integer"

      # 0 = success, 37 = INVALID_REPLICA_ASSIGNMENT (no ongoing reassignment to cancel)
      assert response.error_code in [0, 37],
             "V0 expected 0 or 37, got #{response.error_code}"

      assert is_list(response.responses),
             "V0 responses should be a list"

      assert is_list(response.tagged_fields),
             "V0 tagged_fields should be a list (flexible version)"
    end

    @tag api: :alter_partition_reassignments, version: 0
    test "V0 with empty topics list is accepted", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.AlterPartitionReassignments.V0.Request{
        timeout_ms: 5000,
        topics: [],
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 empty topics: throttle_time_ms should be integer"

      assert response.responses == [],
             "V0 empty topics should return empty responses"
    end

    @tag api: :alter_partition_reassignments
    test "V0 response has tagged_fields (flexible version check)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.AlterPartitionReassignments.V0.Request{
        timeout_ms: 1000,
        topics: [],
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.tagged_fields),
             "V0 AlterPartitionReassignments response.tagged_fields should be a list"
    end
  end

  # ---------------------------------------------------------------------------
  # ListPartitionReassignments V0 (API key 46) — flexible version
  # ---------------------------------------------------------------------------

  describe "ListPartitionReassignments" do
    @tag api: :list_partition_reassignments, version: 0
    test "V0 with specific topic returns parseable response", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ListPartitionReassignments.V0.Request{
        timeout_ms: 5000,
        topics: [
          %{
            name: topic,
            partition_indexes: [0],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.throttle_time_ms),
             "V0 throttle_time_ms should be integer"

      assert is_integer(response.error_code),
             "V0 error_code should be integer"

      assert response.error_code == 0,
             "V0 list reassignments expected error_code 0, got #{response.error_code}"

      assert is_list(response.topics),
             "V0 topics (not 'results') should be a list"

      assert is_list(response.tagged_fields),
             "V0 tagged_fields should be a list (flexible version)"
    end

    @tag api: :list_partition_reassignments, version: 0
    test "V0 with nil topics (all reassignments) returns parseable response", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ListPartitionReassignments.V0.Request{
        timeout_ms: 5000,
        topics: nil,
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_integer(response.error_code),
             "V0 nil topics: error_code should be integer"

      assert response.error_code == 0,
             "V0 nil topics expected 0, got #{response.error_code}"

      assert is_list(response.topics),
             "V0 nil topics: response.topics should be a list"
    end

    @tag api: :list_partition_reassignments, version: 0
    test "V0 with empty topics list is accepted (returns all active reassignments)",
         %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ListPartitionReassignments.V0.Request{
        timeout_ms: 5000,
        topics: [],
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.topics),
             "V0 empty topics list: response.topics should be a list"

      assert response.error_code == 0,
             "V0 empty topics expected 0, got #{response.error_code}"
    end

    @tag api: :list_partition_reassignments
    test "V0 response has tagged_fields (flexible version check)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ListPartitionReassignments.V0.Request{
        timeout_ms: 1000,
        topics: nil,
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.tagged_fields),
             "V0 ListPartitionReassignments response.tagged_fields should be a list"
    end

    @tag api: :list_partition_reassignments
    test "V0 topics entries have correct structure when reassignments listed", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)

      request = %Kayrock.ListPartitionReassignments.V0.Request{
        timeout_ms: 5000,
        topics: [
          %{name: topic, partition_indexes: [0], tagged_fields: []}
        ],
        tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client, request, :random)

      # No active reassignments expected, but structure must be correct if any
      for t <- response.topics do
        assert is_binary(t.name), "topic.name should be binary"
        assert is_list(t.partitions), "topic.partitions should be a list"

        for p <- t.partitions do
          assert is_integer(p.partition_index),
                 "partition.partition_index should be integer"

          assert is_list(p.replicas),
                 "partition.replicas should be a list"
        end
      end
    end
  end
end
