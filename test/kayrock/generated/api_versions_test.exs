defmodule Kayrock.ApiVersionsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.ApiVersionsFactory

  @versions api_version_range(:api_versions)

  describe "versions compatibility" do
    # Test every version with real binaries
    for version <- @versions do
      test "serializes version #{version} request with all fields matching expected binary" do
        version = unquote(version)

        # Get request struct and expected binary from factory
        {request, expected_binary} = ApiVersionsFactory.request_data(version)

        # Serialize and compare
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request binary mismatch:\n#{compare_binaries(serialized, expected_binary)}"

        # Verify request fields are correctly set
        assert request.correlation_id == version
        assert request.client_id == "kayrock-capture"

        if version == 3 do
          assert request.client_software_name == "kayrock"
          assert request.client_software_version == "1.0.0"
        end

        # Verify serialized binary structure
        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 18, "api_key should be 18 (ApiVersions)"
        assert api_version == version, "api_version should match version #{version}"
        assert correlation_id == version, "correlation_id should be #{version}"
      end

      test "deserializes version #{version} response with all fields validated" do
        version = unquote(version)

        # Skip V3 response - broker doesn't support it yet
        if version != 3 do
          response_module = Module.concat([Kayrock.ApiVersions, :"V#{version}", Response])

          # Get response binary and expected struct from factory
          {response_binary, expected_struct} = ApiVersionsFactory.response_data(version)

          # Deserialize response
          {actual_struct, rest} = response_module.deserialize(response_binary)

          # Verify no trailing bytes in captured response
          assert rest == <<>>, "Captured response should have no trailing bytes"

          # Test every field matches expected struct
          assert actual_struct == expected_struct,
                 "V#{version} response struct mismatch"

          # Additional field-level validations
          assert actual_struct.correlation_id == version,
                 "correlation_id should match version #{version}"

          assert actual_struct.error_code == 0, "error_code should be 0 (no error)"

          # Version-specific field validations
          if version in [1, 2] do
            assert Map.has_key?(actual_struct, :throttle_time_ms),
                   "V#{version} should have throttle_time_ms field"
          end
        end
      end
    end
  end

  describe "edge cases" do
    alias Kayrock.ApiVersions.V0.Response

    test "handles max int16 api_key" do
      response_binary =
        ApiVersionsFactory.response_binary(0,
          api_keys: [%{api_key: 32_767, min_version: 0, max_version: 32_767}]
        )

      {response, <<>>} = Response.deserialize(response_binary)
      [entry] = response.api_keys
      assert entry.api_key == 32_767
      assert entry.min_version == 0
      assert entry.max_version == 32_767
    end

    test "handles negative error codes" do
      response_binary = ApiVersionsFactory.error_response(0, error_code: -1)

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.correlation_id == 0
      assert response.error_code == -1
      assert response.api_keys == []
    end

    test "deserializes empty api_keys array" do
      response_binary = ApiVersionsFactory.minimal_response(0, correlation_id: 42)

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.correlation_id == 42
      assert response.error_code == 0
      assert response.api_keys == []
    end

    test "deserializes error response (UNSUPPORTED_VERSION)" do
      response_binary = ApiVersionsFactory.error_response(0, correlation_id: 1, error_code: 35)

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.correlation_id == 1
      assert response.error_code == 35
      assert response.api_keys == []
    end

    test "V1 deserializes response with throttle_time and many APIs" do
      response_binary = ApiVersionsFactory.large_response(1)
      {response, <<>>} = Kayrock.ApiVersions.V1.Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
      assert response.throttle_time_ms == 0
      assert length(response.api_keys) == 10

      api_map = Map.new(response.api_keys, &{&1.api_key, &1})
      assert api_map[0].max_version == 9
      assert api_map[1].max_version == 13
      assert api_map[3].max_version == 12
      assert api_map[8].max_version == 8
      assert api_map[9].max_version == 8
    end

    test "V1 deserializes response with non-zero throttle_time" do
      response_binary =
        ApiVersionsFactory.response_binary(1,
          correlation_id: 5,
          throttle_time_ms: 100,
          api_keys: [%{api_key: 18, min_version: 0, max_version: 3}]
        )

      {response, <<>>} = Kayrock.ApiVersions.V1.Response.deserialize(response_binary)

      assert response.correlation_id == 5
      assert response.error_code == 0
      assert response.throttle_time_ms == 100
      assert [api_versions] = response.api_keys
      assert api_versions.api_key == 18
      assert api_versions.min_version == 0
      assert api_versions.max_version == 3
    end

    test "V3 serializes with compact format fields" do
      {request, expected_binary} = ApiVersionsFactory.v3_empty_fields_request_data()
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary, compare_binaries(serialized, expected_binary)

      assert request.correlation_id == 0
      assert request.client_id == ""
      assert request.client_software_name == ""
      assert request.client_software_version == ""
    end

    test "handles truncated binary at various points" do
      response_binary =
        ApiVersionsFactory.response_binary(0,
          correlation_id: 5,
          api_keys: [
            %{api_key: 0, min_version: 0, max_version: 9},
            %{api_key: 1, min_version: 0, max_version: 13}
          ]
        )

      # Test truncation at various points
      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(Kayrock.ApiVersions.V0.Response, response_binary, truncate_at)
      end
    end

    test "handles extra trailing bytes" do
      # Use factory to generate minimal V0 response
      response_binary =
        ApiVersionsFactory.response_binary(0,
          api_keys: [%{api_key: 0, min_version: 0, max_version: 9}]
        )

      # Add trailing bytes
      extra_bytes = <<1, 2, 3, 4, 5>>

      assert_extra_bytes_returned(
        Kayrock.ApiVersions.V0.Response,
        response_binary,
        extra_bytes
      )
    end

    test "V1 handles truncated binary" do
      # Use factory to generate V1 response
      response_binary =
        ApiVersionsFactory.response_binary(1,
          correlation_id: 1,
          api_keys: [%{api_key: 18, min_version: 0, max_version: 3}]
        )

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(Kayrock.ApiVersions.V1.Response, response_binary, truncate_at)
      end
    end

    test "V1 handles extra trailing bytes" do
      # Use factory with custom throttle_time
      response_binary =
        ApiVersionsFactory.response_binary(1,
          correlation_id: 1,
          throttle_time_ms: 50,
          api_keys: [%{api_key: 18, min_version: 0, max_version: 3}]
        )

      assert_extra_bytes_returned(
        Kayrock.ApiVersions.V1.Response,
        response_binary,
        <<9, 9, 9>>
      )
    end

    test "V2 handles truncated binary" do
      response_binary = ApiVersionsFactory.minimal_response(2, correlation_id: 2)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(Kayrock.ApiVersions.V2.Response, response_binary, truncate_at)
      end
    end

    test "deserializing with invalid array length fails" do
      # Invalid array length (claims 1000 items) causes FunctionClauseError
      # when deserializer tries to read beyond available data
      invalid_binary = <<
        # Valid correlation_id
        0,
        0,
        0,
        0,
        # Valid error_code
        0,
        0,
        # Invalid array length (claims 1000 items but data ends)
        0,
        0,
        3,
        232
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.ApiVersions.V0.Response.deserialize(invalid_binary)
      end
    end

    test "empty binary fails to deserialize" do
      assert_raise MatchError, fn ->
        Kayrock.ApiVersions.V0.Response.deserialize(<<>>)
      end
    end

    test "response with only correlation_id fails" do
      partial = <<0, 0, 0, 0>>

      assert_raise FunctionClauseError, fn ->
        Kayrock.ApiVersions.V0.Response.deserialize(partial)
      end
    end
  end

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- @versions do
        request_module = Module.concat([Kayrock.ApiVersions, :"V#{version}", Request])
        request = struct(request_module, correlation_id: version, client_id: "test")

        assert Kayrock.Request.api_vsn(request) == version,
               "api_vsn should return #{version} for V#{version}.Request"
      end
    end

    test "response_deserializer returns correct function for all versions" do
      for version <- @versions do
        request_module = Module.concat([Kayrock.ApiVersions, :"V#{version}", Request])
        request = struct(request_module, correlation_id: version, client_id: "test")

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1), "V#{version} should have a deserializer function"
      end
    end
  end
end
