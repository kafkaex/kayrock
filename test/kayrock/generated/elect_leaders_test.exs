defmodule Kayrock.ElectLeadersTest do
  @moduledoc """
  Tests for ElectLeaders API (V0-V2).

  API Key: 43
  Used to: Trigger leader election for topic partitions.

  Protocol structure:
  - V0: Basic election
  - V1+: Adds election_type field
  - V2: Adds top-level error_code in response
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.ElectLeadersFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..2 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = ElectLeadersFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 43
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = ElectLeadersFactory.response_data(version)

        response_module = Module.concat([Kayrock.ElectLeaders, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.replica_election_results)
      end
    end

    test "all available versions have modules" do
      for version <- 0..2 do
        request_module = Module.concat([Kayrock, ElectLeaders, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, ElectLeaders, :"V#{version}", Response])

        assert Code.ensure_loaded?(request_module),
               "Request module #{inspect(request_module)} should exist"

        assert Code.ensure_loaded?(response_module),
               "Response module #{inspect(response_module)} should exist"
      end
    end
  end

  # ============================================
  # Version-Specific Features
  # ============================================

  describe "V0 - basic elect leaders" do
    alias Kayrock.ElectLeaders.V0.Request
    alias Kayrock.ElectLeaders.V0.Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topic_partitions: [
          %{topic: "my-topic", partition_id: [0, 1, 2]}
        ],
        timeout_ms: 30_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 43
      assert api_version == 0
    end

    test "response deserializes correctly" do
      {response_binary, expected} = ElectLeadersFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0

      [result] = response.replica_election_results
      assert result.topic == "topic1"
      [partition_result] = result.partition_result
      assert partition_result.error_code == 0
    end
  end

  describe "V1 - adds election_type" do
    alias Kayrock.ElectLeaders.V1.Request
    alias Kayrock.ElectLeaders.V1.Response

    test "request with election_type serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        election_type: 0,
        topic_partitions: [
          %{topic: "topic1", partition_id: [0]}
        ],
        timeout_ms: 30_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 43
      assert api_version == 1
    end

    test "response with throttle_time_ms deserializes correctly" do
      {response_binary, expected} = ElectLeadersFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50
    end
  end

  describe "V2 - adds top-level error_code" do
    alias Kayrock.ElectLeaders.V2.Response

    test "response with top-level error_code deserializes correctly" do
      {response_binary, expected} = ElectLeadersFactory.response_data(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.error_code == 0
      assert response.throttle_time_ms == 100
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, ElectLeaders, :"V#{version}", Request])

        base_fields = [
          correlation_id: 0,
          client_id: "test",
          topic_partitions: [],
          timeout_ms: 30_000
        ]

        fields =
          cond do
            version >= 2 ->
              base_fields
              |> Keyword.put(:election_type, 0)
              |> Keyword.put(:tagged_fields, [])

            version >= 1 ->
              Keyword.put(base_fields, :election_type, 0)

            true ->
              base_fields
          end

        request = struct(module, fields)
        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, ElectLeaders, :"V#{version}", Request])

        base_fields = [
          correlation_id: 0,
          client_id: "test",
          topic_partitions: [],
          timeout_ms: 30_000
        ]

        fields =
          cond do
            version >= 2 ->
              base_fields
              |> Keyword.put(:election_type, 0)
              |> Keyword.put(:tagged_fields, [])

            version >= 1 ->
              Keyword.put(base_fields, :election_type, 0)

            true ->
              base_fields
          end

        request = struct(module, fields)
        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..2 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = ElectLeadersFactory.response_data(version)
        response_module = Module.concat([Kayrock.ElectLeaders, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    for version <- 0..2 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = ElectLeadersFactory.response_data(version)
        response_module = Module.concat([Kayrock.ElectLeaders, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  describe "malformed response handling" do
    for version <- 0..2 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.ElectLeaders, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  describe "error code handling" do
    @error_codes [
      {0, 84, "NO_REASSIGNMENT_IN_PROGRESS"},
      {1, 85, "ELECTION_NOT_NEEDED"},
      {2, 38, "ELIGIBLE_LEADERS_NOT_AVAILABLE"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.ElectLeaders, :"V#{version}", Response])

        response_binary = ElectLeadersFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [result] = response.replica_election_results
        [partition_result] = result.partition_result
        assert partition_result.error_code == error_code
        assert partition_result.error_message == "Election failed"
      end
    end
  end
end
