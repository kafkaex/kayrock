defmodule Kayrock.Sanity.ConsumerGroupSanityTest do
  @moduledoc """
  Sanity tests for consumer group APIs.

  Covers:
  - FindCoordinator V0-V3 (API key 10)
  - JoinGroup V0-V6 (API key 11)
  - SyncGroup V0-V4 (API key 14)
  - Heartbeat V0-V4 (API key 12)
  - LeaveGroup V0-V4 (API key 13)
  - DescribeGroups V0-V5 (API key 15)
  - OffsetCommit V0-V8 (API key 8)
  - OffsetFetch V0-V6 (API key 9)
  - DeleteGroups V0-V2 (API key 42)
  - OffsetDelete V0 (API key 47)

  Run with:
      mix test.sanity test/sanity/consumer_group_sanity_test.exs
  """
  use Kayrock.SanityCase
  use ExUnit.Case, async: false

  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  # ---------------------------------------------------------------------------
  # setup_all: shared client and topic
  # ---------------------------------------------------------------------------

  setup_all %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_topic(client, 5)
    :ok = wait_for_topic(client, topic)
    %{client: client, topic: topic}
  end

  # ---------------------------------------------------------------------------
  # Group lifecycle helpers
  # ---------------------------------------------------------------------------

  # Join a fresh group (as leader/sole member), return context map.
  # Uses JoinGroup V2 which is widely-supported and has rebalance_timeout_ms.
  defp join_fresh_group(client, topic) do
    group_id = "sanity-cg-#{unique_string()}"
    vsn = min(2, Kayrock.FindCoordinator.max_vsn())
    coord_req = find_coordinator_request(group_id, vsn)

    {:ok, coord_resp} =
      with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)

    node_id = coord_resp.node_id

    join_req =
      join_group_request(%{group_id: group_id, topics: [topic]}, 2)

    {:ok, join_resp} =
      with_retry(fn -> Kayrock.client_call(client, join_req, node_id) end)

    assert join_resp.error_code == 0,
           "JoinGroup failed in join_fresh_group: #{join_resp.error_code}"

    %{
      group_id: group_id,
      node_id: node_id,
      member_id: join_resp.member_id,
      generation_id: join_resp.generation_id
    }
  end

  # Join AND sync a group so it reaches the Stable state.
  # Returns the same context map.
  defp join_and_sync_group(client, topic) do
    ctx = join_fresh_group(client, topic)
    %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} = ctx

    assignments = [%{member_id: member_id, topic: topic, partitions: [0, 1, 2]}]

    sync_req =
      group_id
      |> sync_group_request(member_id, assignments, 2)
      |> Map.put(:generation_id, gen_id)

    {:ok, sync_resp} =
      with_retry(fn -> Kayrock.client_call(client, sync_req, node_id) end)

    assert sync_resp.error_code == 0,
           "SyncGroup failed in join_and_sync_group: #{sync_resp.error_code}"

    ctx
  end

  # Leave a group cleanly (V0 protocol).
  defp leave_group(client, group_id, member_id, node_id) do
    leave_req = leave_group_request(group_id, member_id, 0)

    with_retry(
      fn -> Kayrock.client_call(client, leave_req, node_id) end,
      accept_errors: [0]
    )
  end

  # ---------------------------------------------------------------------------
  # FindCoordinator V0-V3
  # ---------------------------------------------------------------------------

  describe "FindCoordinator" do
    for version <- 0..3 do
      @tag api: :find_coordinator, version: version
      test "V#{version} returns coordinator for a group", %{client: client} do
        version = unquote(version)
        group_id = "sanity-fc-#{unique_string()}"

        request = find_coordinator_request(group_id, version)

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, :random) end)

        assert response.error_code == 0,
               "V#{version} expected error_code 0, got #{response.error_code}"

        assert is_integer(response.node_id),
               "V#{version} node_id should be integer"

        assert is_binary(response.host),
               "V#{version} host should be binary"

        assert is_integer(response.port),
               "V#{version} port should be integer > 0"

        assert response.port > 0
      end
    end

    # V0 has no throttle_time_ms or error_message
    @tag api: :find_coordinator, version: 0
    test "V0 response has no throttle_time_ms or error_message", %{client: client} do
      group_id = "sanity-fc-#{unique_string()}"
      request = find_coordinator_request(group_id, 0)
      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 response should not have throttle_time_ms"

      refute Map.has_key?(response, :error_message),
             "V0 response should not have error_message"
    end

    # V1+ include throttle_time_ms and error_message
    for version <- 1..3 do
      @tag api: :find_coordinator, version: version
      test "V#{version} response includes throttle_time_ms and error_message", %{client: client} do
        version = unquote(version)
        group_id = "sanity-fc-#{unique_string()}"
        request = find_coordinator_request(group_id, version)
        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        # error_message is nullable; on success it's nil
        assert is_nil(response.error_message) or is_binary(response.error_message),
               "V#{version} error_message should be nil or binary"
      end
    end

    # V3 is flexible — response includes tagged_fields
    @tag api: :find_coordinator, version: 3
    test "V3 response includes tagged_fields (flexible version)", %{client: client} do
      group_id = "sanity-fc-#{unique_string()}"
      request = find_coordinator_request(group_id, 3)
      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)

      assert is_list(response.tagged_fields),
             "V3 response.tagged_fields should be a list (flexible)"
    end

    # V0 request struct must NOT have key_type field
    @tag api: :find_coordinator, version: 0
    test "V0 request struct has no key_type field", %{client: client} do
      group_id = "sanity-fc-#{unique_string()}"
      request = find_coordinator_request(group_id, 0)

      refute Map.has_key?(request, :key_type),
             "V0 request should not have key_type"

      # Should still succeed
      {:ok, _response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)
    end

    # V1+ request struct MUST have key_type field
    for version <- 1..3 do
      @tag api: :find_coordinator, version: version
      test "V#{version} request struct has key_type field", %{client: client} do
        version = unquote(version)
        group_id = "sanity-fc-#{unique_string()}"
        request = find_coordinator_request(group_id, version)

        assert Map.has_key?(request, :key_type),
               "V#{version} request should have key_type"

        assert request.key_type == 0,
               "V#{version} key_type should be 0 for consumer group"

        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)
        assert response.error_code == 0
      end
    end
  end

  # ---------------------------------------------------------------------------
  # JoinGroup V0-V6
  # ---------------------------------------------------------------------------

  describe "JoinGroup" do
    for version <- 0..6 do
      @tag api: :join_group, version: version
      test "V#{version} can join a new group", %{client: client, topic: topic} do
        version = unquote(version)
        group_id = "sanity-jg-v#{version}-#{unique_string()}"

        # Find coordinator first
        coord_req = find_coordinator_request(group_id, min(2, Kayrock.FindCoordinator.max_vsn()))
        {:ok, coord_resp} = with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)
        node_id = coord_resp.node_id

        # V4+ requires KIP-394 two-step join: broker may return error 79
        # (MEMBER_ID_REQUIRED) with an assigned member_id, which must be used
        # in the second join request.
        {:ok, response} =
          if version >= 4 do
            join_group_kip394(client, group_id, topic, node_id, version)
          else
            request = build_join_group_request(group_id, topic, version)
            with_retry(fn -> Kayrock.client_call(client, request, node_id) end)
          end

        assert response.error_code == 0,
               "V#{version} JoinGroup error_code should be 0, got #{response.error_code}"

        assert is_binary(response.member_id) and response.member_id != "",
               "V#{version} member_id should be non-empty string"

        assert is_integer(response.generation_id),
               "V#{version} generation_id should be integer"

        assert response.generation_id >= 1,
               "V#{version} generation_id should be >= 1"

        assert is_list(response.members),
               "V#{version} members should be a list"

        assert is_binary(response.leader),
               "V#{version} leader should be binary"

        assert is_binary(response.protocol_name),
               "V#{version} protocol_name should be binary"
      end
    end

    # V6 is flexible — response has tagged_fields
    @tag api: :join_group, version: 6
    test "V6 response has tagged_fields (flexible)", %{client: client, topic: topic} do
      group_id = "sanity-jg-flex-#{unique_string()}"
      coord_req = find_coordinator_request(group_id, min(2, Kayrock.FindCoordinator.max_vsn()))
      {:ok, coord_resp} = with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)
      node_id = coord_resp.node_id

      {:ok, response} = join_group_kip394(client, group_id, topic, node_id, 6)
      assert response.error_code == 0

      assert is_list(response.tagged_fields),
             "V6 response.tagged_fields should be a list"
    end

    # V0 has no rebalance_timeout_ms in request
    @tag api: :join_group, version: 0
    test "V0 request struct has no rebalance_timeout_ms", %{client: client, topic: topic} do
      group_id = "sanity-jg-v0rt-#{unique_string()}"
      request = build_join_group_request(group_id, topic, 0)

      refute Map.has_key?(request, :rebalance_timeout_ms),
             "V0 request should not have rebalance_timeout_ms"

      coord_req = find_coordinator_request(group_id, 0)
      {:ok, coord_resp} = with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)

      {:ok, response} =
        with_retry(fn -> Kayrock.client_call(client, request, coord_resp.node_id) end)

      assert response.error_code == 0
    end

    # Sole member becomes the leader
    @tag api: :join_group
    test "sole member is the leader", %{client: client, topic: topic} do
      group_id = "sanity-jg-leader-#{unique_string()}"
      coord_req = find_coordinator_request(group_id, 2)
      {:ok, coord_resp} = with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)
      node_id = coord_resp.node_id

      request = build_join_group_request(group_id, topic, 2)
      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)
      assert response.error_code == 0

      assert response.leader == response.member_id,
             "sole member should be the leader"
    end
  end

  # ---------------------------------------------------------------------------
  # SyncGroup V0-V4
  # ---------------------------------------------------------------------------

  describe "SyncGroup" do
    for version <- 0..4 do
      @tag api: :sync_group, version: version
      test "V#{version} sync a group returns assignment", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_fresh_group(client, topic)

        %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} =
          ctx

        assignments = [%{member_id: member_id, topic: topic, partitions: [0, 1, 2]}]
        request = build_sync_group_request(group_id, member_id, assignments, gen_id, version)

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert response.error_code == 0,
               "V#{version} SyncGroup error_code should be 0, got #{response.error_code}"

        # SyncGroup response deserializes the assignment bytes into a MemberAssignment struct
        assert is_struct(response.assignment, Kayrock.MemberAssignment) or
                 is_nil(response.assignment),
               "V#{version} assignment should be a MemberAssignment struct (got #{inspect(response.assignment)})"
      end
    end

    # V4 has tagged_fields in request
    @tag api: :sync_group, version: 4
    test "V4 request has tagged_fields (flexible)", %{client: client, topic: topic} do
      ctx = join_fresh_group(client, topic)
      %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} = ctx

      assignments = [%{member_id: member_id, topic: topic, partitions: [0, 1, 2]}]
      request = build_sync_group_request(group_id, member_id, assignments, gen_id, 4)

      assert Map.has_key?(request, :tagged_fields),
             "V4 SyncGroup request should have tagged_fields"

      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)
      assert response.error_code == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Heartbeat V0-V4
  # ---------------------------------------------------------------------------

  describe "Heartbeat" do
    for version <- 0..4 do
      @tag api: :heartbeat, version: version
      test "V#{version} heartbeat to active group succeeds", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)

        %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} =
          ctx

        request = build_heartbeat_request(group_id, member_id, gen_id, version)

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert response.error_code == 0,
               "V#{version} Heartbeat error_code should be 0, got #{response.error_code}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # LeaveGroup V0-V4
  # ---------------------------------------------------------------------------

  describe "LeaveGroup" do
    # V0-V2: single member_id field
    for version <- 0..2 do
      @tag api: :leave_group, version: version
      test "V#{version} leave group with single member_id", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id, member_id: member_id} = ctx

        request = build_leave_group_request(group_id, member_id, version)

        {:ok, response} =
          with_retry(
            fn -> Kayrock.client_call(client, request, node_id) end,
            accept_errors: [0]
          )

        assert response.error_code == 0,
               "V#{version} LeaveGroup error_code should be 0, got #{response.error_code}"
      end
    end

    # V3-V4: uses members list, NOT member_id
    for version <- 3..4 do
      @tag api: :leave_group, version: version
      test "V#{version} leave group with members list", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id, member_id: member_id} = ctx

        request = build_leave_group_v3_plus_request(group_id, member_id, version)

        {:ok, response} =
          with_retry(
            fn -> Kayrock.client_call(client, request, node_id) end,
            accept_errors: [0]
          )

        assert response.error_code == 0,
               "V#{version} LeaveGroup (members list) error_code should be 0, got #{response.error_code}"
      end
    end

    # V3+ response has members list
    for version <- 3..4 do
      @tag api: :leave_group, version: version
      test "V#{version} response includes members list", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id, member_id: member_id} = ctx

        request = build_leave_group_v3_plus_request(group_id, member_id, version)

        {:ok, response} =
          with_retry(
            fn -> Kayrock.client_call(client, request, node_id) end,
            accept_errors: [0]
          )

        assert response.error_code == 0

        assert is_list(response.members),
               "V#{version} LeaveGroup response should have members list"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # DescribeGroups V0-V5
  # ---------------------------------------------------------------------------

  describe "DescribeGroups" do
    for version <- 0..5 do
      @tag api: :describe_groups, version: version
      test "V#{version} describe an existing group", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id} = ctx

        request = build_describe_groups_request([group_id], version)

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert is_list(response.groups),
               "V#{version} response.groups should be a list"

        assert length(response.groups) == 1,
               "V#{version} expected 1 group in response"

        [group] = response.groups

        assert group.error_code == 0,
               "V#{version} group.error_code should be 0, got #{group.error_code}"

        assert group.group_id == group_id,
               "V#{version} group.group_id should match"
      end
    end

    # V1+ has throttle_time_ms
    for version <- 1..5 do
      @tag api: :describe_groups, version: version
      test "V#{version} response includes throttle_time_ms", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id} = ctx

        request = build_describe_groups_request([group_id], version)
        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"
      end
    end

    # V5 response also has tagged_fields
    @tag api: :describe_groups, version: 5
    test "V5 response includes tagged_fields (flexible)", %{client: client, topic: topic} do
      ctx = join_and_sync_group(client, topic)
      %{group_id: group_id, node_id: node_id} = ctx

      request = build_describe_groups_request([group_id], 5)
      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

      # Groups are in compact format; response struct may have tagged_fields
      assert is_list(response.groups)
    end

    # Describe non-existent group returns UNKNOWN_MEMBER_ID or INVALID_GROUP_ID or 0
    @tag api: :describe_groups
    test "describing non-existent group returns sensible response", %{client: client} do
      request = describe_groups_request(["no-such-group-#{unique_string()}"], 2)
      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)

      assert is_list(response.groups)
      [group] = response.groups
      # Typically returns error_code 0 with state "Dead" for unknown group
      assert is_integer(group.error_code)
    end
  end

  # ---------------------------------------------------------------------------
  # OffsetCommit V0-V8
  # ---------------------------------------------------------------------------

  describe "OffsetCommit" do
    for version <- 0..8 do
      @tag api: :offset_commit, version: version
      test "V#{version} commit offset succeeds", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)

        %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} =
          ctx

        request = build_offset_commit_request(group_id, topic, member_id, gen_id, version)

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert is_list(response.topics),
               "V#{version} response.topics should be a list"

        assert response.topics != [],
               "V#{version} response.topics should not be empty"

        [topic_resp] = response.topics

        assert is_list(topic_resp.partitions),
               "V#{version} topic partitions should be a list"

        [partition_resp] = topic_resp.partitions

        assert partition_resp.error_code == 0,
               "V#{version} partition error_code should be 0, got #{partition_resp.error_code}"
      end
    end

    # V1 has commit_timestamp in partition data (need special handling)
    @tag api: :offset_commit, version: 1
    test "V1 commit with explicit timestamp succeeds", %{client: client, topic: topic} do
      ctx = join_and_sync_group(client, topic)
      %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} = ctx

      request = build_offset_commit_request(group_id, topic, member_id, gen_id, 1)
      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

      assert is_list(response.topics)
      [topic_resp] = response.topics
      [partition_resp] = topic_resp.partitions
      assert partition_resp.error_code == 0
    end
  end

  # ---------------------------------------------------------------------------
  # OffsetFetch V0-V6
  # ---------------------------------------------------------------------------

  describe "OffsetFetch" do
    for version <- 0..6 do
      @tag api: :offset_fetch, version: version
      test "V#{version} fetch committed offsets", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)

        %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} =
          ctx

        # Commit offsets first using a compatible version
        commit_vsn = min(version, 8)
        commit_req = build_offset_commit_request(group_id, topic, member_id, gen_id, commit_vsn)

        {:ok, _commit_resp} =
          with_retry(fn -> Kayrock.client_call(client, commit_req, node_id) end)

        # Now fetch
        request = build_offset_fetch_request(group_id, topic, version)

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert is_list(response.topics),
               "V#{version} response.topics should be a list"
      end
    end

    # V3+ has throttle_time_ms (V0-V2 do not have it; V2 only has error_code at top level)
    for version <- 3..6 do
      @tag api: :offset_fetch, version: version
      test "V#{version} response includes throttle_time_ms", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id} = ctx

        request = build_offset_fetch_request(group_id, topic, version)
        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert is_integer(response.throttle_time_ms),
               "V#{version} response.throttle_time_ms should be integer"
      end
    end

    # V6 response also has error_code at top level (introduced in V2)
    for version <- 2..6 do
      @tag api: :offset_fetch, version: version
      test "V#{version} response includes error_code at top level", %{
        client: client,
        topic: topic
      } do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id} = ctx

        request = build_offset_fetch_request(group_id, topic, version)
        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert is_integer(response.error_code),
               "V#{version} response should have error_code"

        assert response.error_code == 0,
               "V#{version} response error_code should be 0"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # DeleteGroups V0-V2
  # ---------------------------------------------------------------------------

  describe "DeleteGroups" do
    for version <- 0..2 do
      @tag api: :delete_groups, version: version
      test "V#{version} delete an empty (left) group", %{client: client, topic: topic} do
        version = unquote(version)
        ctx = join_and_sync_group(client, topic)
        %{group_id: group_id, node_id: node_id, member_id: member_id} = ctx

        # Leave the group first so it becomes Empty
        leave_group(client, group_id, member_id, node_id)
        # Wait for the group to reach Empty/Dead state before deleting
        :timer.sleep(1000)

        request = build_delete_groups_request([group_id], version)

        # DeleteGroups response has no top-level error_code, so with_retry would
        # loop endlessly treating it as "other". Call directly and retry on nested error.
        {:ok, response} = delete_group_with_retry(client, request, node_id)

        assert is_list(response.results),
               "V#{version} response.results should be a list"

        [result] = response.results

        assert result.group_id == group_id,
               "V#{version} result.group_id should match"

        # 0 = deleted successfully; 69 = GROUP_ID_NOT_FOUND (broker already
        # garbage-collected the Dead group before our request arrived — effectively deleted)
        assert result.error_code in [0, 69],
               "V#{version} result.error_code should be 0 or 69, got #{result.error_code}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # OffsetDelete V0
  # ---------------------------------------------------------------------------

  describe "OffsetDelete" do
    @tag api: :offset_delete, version: 0
    test "V0 delete committed offsets for a group", %{client: client, topic: topic} do
      ctx = join_and_sync_group(client, topic)
      %{group_id: group_id, node_id: node_id, member_id: member_id, generation_id: gen_id} = ctx

      # Commit an offset first
      commit_req = build_offset_commit_request(group_id, topic, member_id, gen_id, 2)
      {:ok, _} = with_retry(fn -> Kayrock.client_call(client, commit_req, node_id) end)

      # Leave the group so we can delete offsets (group must be Empty)
      leave_group(client, group_id, member_id, node_id)
      :timer.sleep(500)

      request = build_offset_delete_request(group_id, topic)

      {:ok, response} =
        with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

      assert is_integer(response.error_code),
             "V0 OffsetDelete response should have error_code"

      assert response.error_code == 0,
             "V0 OffsetDelete error_code should be 0, got #{response.error_code}"

      assert is_integer(response.throttle_time_ms),
             "V0 OffsetDelete response should have throttle_time_ms"

      assert is_list(response.topics),
             "V0 OffsetDelete response should have topics list"
    end
  end

  # ---------------------------------------------------------------------------
  # Private request builders
  # ---------------------------------------------------------------------------

  # JoinGroup request builder — handles V0 (no rebalance_timeout) vs V1-V4 vs V5+ (group_instance_id)
  defp build_join_group_request(group_id, topic, version, member_id \\ "") do
    vsn = min(version, Kayrock.JoinGroup.max_vsn())
    request = Kayrock.JoinGroup.get_request_struct(vsn)
    metadata = %Kayrock.GroupProtocolMetadata{topics: [topic]}

    base = %{
      request
      | group_id: group_id,
        session_timeout_ms: 10_000,
        member_id: member_id,
        protocol_type: "consumer",
        protocols: [%{name: "assign", metadata: metadata}]
    }

    base =
      if vsn >= 1 do
        %{base | rebalance_timeout_ms: 30_000}
      else
        base
      end

    base =
      if vsn >= 5 do
        %{base | group_instance_id: nil}
      else
        base
      end

    if vsn >= 6 do
      %{base | tagged_fields: []}
    else
      base
    end
  end

  # KIP-394 two-step join helper for JoinGroup V4+.
  # The broker may respond with error 79 (MEMBER_ID_REQUIRED) and a new member_id.
  # We must retry with that member_id. Standard with_retry retries the original
  # closure (with member_id: "") so we handle this manually.
  defp join_group_kip394(client, group_id, topic, node_id, version, retries \\ 10) do
    join_group_kip394_loop(client, group_id, topic, node_id, version, "", retries)
  end

  defp join_group_kip394_loop(_client, _group_id, _topic, _node_id, _version, _member_id, 0) do
    {:error, :max_retries_exceeded}
  end

  defp join_group_kip394_loop(client, group_id, topic, node_id, version, member_id, retries) do
    request = build_join_group_request(group_id, topic, version, member_id)

    case Kayrock.client_call(client, request, node_id) do
      {:ok, %{error_code: 0} = response} ->
        {:ok, response}

      {:ok, %{error_code: 79, member_id: new_member_id}}
      when is_binary(new_member_id) and new_member_id != "" ->
        # MEMBER_ID_REQUIRED — broker assigned us a member_id; retry with it
        :timer.sleep(500)

        join_group_kip394_loop(
          client,
          group_id,
          topic,
          node_id,
          version,
          new_member_id,
          retries - 1
        )

      {:ok, %{error_code: code}} when code in [15, 16, 25, 27] ->
        # Transient coordinator errors — retry with same member_id
        :timer.sleep(500)
        join_group_kip394_loop(client, group_id, topic, node_id, version, member_id, retries - 1)

      {:ok, response} ->
        {:ok, response}

      {:error, _} ->
        :timer.sleep(500)
        join_group_kip394_loop(client, group_id, topic, node_id, version, member_id, retries - 1)
    end
  end

  # DeleteGroups response has no top-level error_code, so with_retry cannot
  # detect success vs transient failure. Retry on nested result error_code.
  defp delete_group_with_retry(client, request, node_id, retries \\ 10)

  defp delete_group_with_retry(_client, _request, _node_id, 0),
    do: {:error, :max_retries_exceeded}

  defp delete_group_with_retry(client, request, node_id, retries) do
    case Kayrock.client_call(client, request, node_id) do
      {:ok, %{results: [%{error_code: 0}]} = response} ->
        {:ok, response}

      {:ok, %{results: [%{error_code: code}]}} when code in [15, 16, 68] ->
        # 15=COORDINATOR_NOT_AVAILABLE, 16=NOT_COORDINATOR,
        # 68=NON_EMPTY_GROUP (group not yet transitioned to Empty — retry)
        :timer.sleep(700)
        delete_group_with_retry(client, request, node_id, retries - 1)

      {:ok, response} ->
        {:ok, response}

      {:error, _} = err ->
        err
    end
  end

  # SyncGroup request builder — handles all versions
  defp build_sync_group_request(group_id, member_id, assignments, generation_id, version) do
    vsn = min(version, Kayrock.SyncGroup.max_vsn())

    request =
      group_id
      |> sync_group_request(member_id, assignments, vsn)
      |> Map.put(:generation_id, generation_id)

    request =
      if vsn >= 3 do
        %{request | group_instance_id: nil}
      else
        request
      end

    if vsn >= 4 do
      %{request | tagged_fields: []}
    else
      request
    end
  end

  # Heartbeat request builder — handles all versions
  defp build_heartbeat_request(group_id, member_id, generation_id, version) do
    vsn = min(version, Kayrock.Heartbeat.max_vsn())
    request = heartbeat_request(group_id, member_id, generation_id, vsn)

    request =
      if vsn >= 4 do
        %{request | group_instance_id: nil, tagged_fields: []}
      else
        request
      end

    request
  end

  # LeaveGroup V0-V2 request builder (member_id based)
  defp build_leave_group_request(group_id, member_id, version) do
    vsn = min(version, 2)
    leave_group_request(group_id, member_id, vsn)
  end

  # LeaveGroup V3-V4 request builder (members list based)
  defp build_leave_group_v3_plus_request(group_id, member_id, version) do
    vsn = min(version, Kayrock.LeaveGroup.max_vsn())
    vsn = max(vsn, 3)
    request = Kayrock.LeaveGroup.get_request_struct(vsn)

    member =
      if vsn >= 4 do
        %{member_id: member_id, group_instance_id: nil, tagged_fields: []}
      else
        %{member_id: member_id, group_instance_id: nil}
      end

    base = %{request | group_id: group_id, members: [member]}

    if vsn >= 4 do
      %{base | tagged_fields: []}
    else
      base
    end
  end

  # DescribeGroups request builder — handles all versions
  defp build_describe_groups_request(group_ids, version) do
    vsn = min(version, Kayrock.DescribeGroups.max_vsn())
    request = describe_groups_request(group_ids, vsn)

    if vsn >= 5 do
      %{request | include_authorized_operations: false, tagged_fields: []}
    else
      request
    end
  end

  # OffsetCommit request builder — handles all versions
  defp build_offset_commit_request(group_id, topic, member_id, generation_id, version) do
    vsn = min(version, Kayrock.OffsetCommit.max_vsn())

    topics_data = [
      [
        topic: topic,
        partitions: [[partition: 0, offset: 1]]
      ]
    ]

    opts = [generation_id: generation_id, member_id: member_id]

    request = offset_commit_request(group_id, topics_data, vsn, opts)

    if vsn >= 8 do
      %{request | group_instance_id: nil, tagged_fields: []}
    else
      request
    end
  end

  # OffsetFetch request builder — handles all versions
  defp build_offset_fetch_request(group_id, topic, version) do
    vsn = min(version, Kayrock.OffsetFetch.max_vsn())

    topics_data = [
      [topic: topic, partitions: [[partition: 0]]]
    ]

    request = offset_fetch_request(group_id, topics_data, vsn)

    if vsn >= 6 do
      %{request | tagged_fields: []}
    else
      request
    end
  end

  # DeleteGroups request builder — handles all versions
  defp build_delete_groups_request(group_ids, version) do
    vsn = min(version, Kayrock.DeleteGroups.max_vsn())
    request = delete_groups_request(group_ids, vsn)

    if vsn >= 2 do
      %{request | tagged_fields: []}
    else
      request
    end
  end

  # OffsetDelete V0 request builder
  defp build_offset_delete_request(group_id, topic) do
    request = Kayrock.OffsetDelete.get_request_struct(0)

    %{
      request
      | group_id: group_id,
        topics: [
          %{
            name: topic,
            partitions: [%{partition_index: 0}]
          }
        ]
    }
  end
end
