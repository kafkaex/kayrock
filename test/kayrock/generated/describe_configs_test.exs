defmodule Kayrock.DescribeConfigsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.DescribeConfigsFactory

  # ============================================
  # Factory-based Tests (Gold Standard)
  # ============================================

  describe "request serialization" do
    test "all versions serialize correctly" do
      for version <- 0..2 do
        {request, expected_binary} = DescribeConfigsFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} serialization mismatch:\n#{compare_binaries(serialized, expected_binary)}"

        # Verify API key and version in header
        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 32
        assert api_version == version
      end
    end
  end

  describe "response deserialization" do
    test "all versions deserialize correctly" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])
        {response_binary, expected_struct} = DescribeConfigsFactory.response_data(version)

        {actual, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>, "V#{version} should consume entire binary"
        assert actual == expected_struct, "V#{version} deserialization mismatch"
        assert actual.correlation_id == version
      end
    end
  end

  describe "round-trip" do
    test "all versions can round-trip through serialization" do
      for version <- 0..2 do
        {request, _expected_binary} = DescribeConfigsFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        # Verify we can at least parse the header
        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized

        assert api_key == 32
        assert api_version == version
        assert correlation_id == version
      end
    end
  end

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    test "all supported versions have request modules" do
      for version <- api_version_range(:describe_configs) do
        module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          resources: []
        }

        fields =
          cond do
            version >= 1 ->
              Map.put(base, :include_synonyms, false)

            true ->
              base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 32
        assert api_version == version
      end
    end

    test "all supported versions have response modules" do
      for version <- api_version_range(:describe_configs) do
        module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])
        assert Code.ensure_loaded?(module), "Response module #{inspect(module)} should exist"
      end
    end
  end

  # ============================================
  # Version-Specific Scenarios
  # ============================================

  describe "V0 specific features" do
    alias Kayrock.DescribeConfigs.V0.Request
    alias Kayrock.DescribeConfigs.V0.Response

    test "request without config_names (null)" do
      request = %Request{
        correlation_id: 100,
        client_id: "test",
        resources: [
          %{
            resource_type: 2,
            resource_name: "my-topic",
            config_names: nil
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      # Verify null array is at the end
      # Structure: api_key(2) + api_vsn(2) + corr_id(4) + client_id_len(2) + client_id(4) +
      #            resources_len(4) + resource_type(1) + name_len(2) + name(8) + config_names_len(4)
      expected_offset = 2 + 2 + 4 + 2 + 4 + 4 + 1 + 2 + 8
      <<_prefix::binary-size(expected_offset), config_names_len::32-signed>> = serialized

      # Null array is encoded as -1
      assert config_names_len == -1
    end

    test "request with specific config_names" do
      request = %Request{
        correlation_id: 101,
        client_id: "test",
        resources: [
          %{
            resource_type: 2,
            resource_name: "topic",
            config_names: ["retention.ms", "cleanup.policy"]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      # Should contain array length of 2
      assert serialized =~ "retention.ms"
      assert serialized =~ "cleanup.policy"
    end

    test "request for broker config" do
      request = %Request{
        correlation_id: 102,
        client_id: "test",
        resources: [
          %{
            # 4 = BROKER
            resource_type: 4,
            resource_name: "0",
            config_names: nil
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response has is_default field (not in V1+)" do
      {_binary, response} = DescribeConfigsFactory.response_data(0)
      [resource] = response.resources
      [config] = resource.config_entries

      assert Map.has_key?(config, :is_default)
      refute Map.has_key?(config, :config_source)
    end
  end

  describe "V1 specific features" do
    alias Kayrock.DescribeConfigs.V1.Request
    alias Kayrock.DescribeConfigs.V1.Response

    test "request with include_synonyms flag" do
      request = %Request{
        correlation_id: 200,
        client_id: "test",
        resources: [],
        include_synonyms: true
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      # Last byte should be 1 (true)
      <<_rest::binary-size(byte_size(serialized) - 1), include_synonyms::8>> = serialized
      assert include_synonyms == 1
    end

    test "response has config_source and config_synonyms (not in V0)" do
      {_binary, response} = DescribeConfigsFactory.response_data(1)
      [resource] = response.resources
      [config] = resource.config_entries

      assert Map.has_key?(config, :config_source)
      assert Map.has_key?(config, :config_synonyms)
      refute Map.has_key?(config, :is_default)
    end

    test "response with config synonyms" do
      response_binary =
        DescribeConfigsFactory.response_binary(1,
          resources: [
            %{
              error_code: 0,
              error_message: nil,
              resource_type: 2,
              resource_name: "topic",
              config_entries: [
                %{
                  config_name: "retention.ms",
                  config_value: "86400000",
                  read_only: 0,
                  config_source: 1,
                  is_sensitive: 0,
                  config_synonyms: [
                    %{
                      config_name: "log.retention.ms",
                      config_value: "86400000",
                      config_source: 5
                    }
                  ]
                }
              ]
            }
          ]
        )

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 1
      [resource] = response.resources
      [config] = resource.config_entries
      [synonym] = config.config_synonyms
      assert synonym.config_name == "log.retention.ms"
      assert synonym.config_value == "86400000"
      assert synonym.config_source == 5
    end
  end

  describe "V2 specific features" do
    alias Kayrock.DescribeConfigs.V2.Request
    alias Kayrock.DescribeConfigs.V2.Response

    test "request structure same as V1 (include_documentation not in struct)" do
      request = %Request{
        correlation_id: 300,
        client_id: "test",
        resources: [],
        include_synonyms: false
      }

      # V2 request has same fields as V1
      refute Map.has_key?(request, :include_documentation)

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 32
      assert api_version == 2
    end

    test "response structure same as V1" do
      {_binary, response} = DescribeConfigsFactory.response_data(2)
      [resource] = response.resources
      [config] = resource.config_entries

      # V2 response has same fields as V1
      assert Map.has_key?(config, :config_source)
      assert Map.has_key?(config, :config_synonyms)
      refute Map.has_key?(config, :is_default)
    end
  end

  # ============================================
  # Edge Cases - Truncated Binary
  # ============================================

  describe "truncated binary handling" do
    test "all versions handle truncated binaries" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])
        {response_binary, _expected_struct} = DescribeConfigsFactory.response_data(version)

        for truncate_at <- truncation_points(response_binary) do
          truncated = truncate_binary(response_binary, truncate_at)

          result =
            try do
              response_module.deserialize(truncated)
              :no_error
            rescue
              e in [MatchError, FunctionClauseError] -> {:error, e.__struct__}
            end

          assert match?({:error, _}, result),
                 "V#{version} at truncate #{truncate_at}: Expected error but got #{inspect(result)}"
        end
      end
    end

    test "empty binary raises MatchError" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  # ============================================
  # Edge Cases - Extra Bytes
  # ============================================

  describe "extra bytes handling" do
    test "all versions return extra bytes as rest" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])
        {response_binary, _expected_struct} = DescribeConfigsFactory.response_data(version)

        extra_bytes = <<111, 222, 333>>
        with_extra = response_binary <> extra_bytes

        {_response, rest} = response_module.deserialize(with_extra)
        assert rest == extra_bytes
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Data
  # ============================================

  describe "malformed response handling" do
    test "invalid resources array length fails" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])

        invalid = <<
          # correlation_id
          0,
          0,
          0,
          0,
          # throttle_time_ms
          0,
          0,
          0,
          0,
          # Claims 600 resources
          0,
          0,
          2,
          88
        >>

        result =
          try do
            response_module.deserialize(invalid)
            :no_error
          rescue
            e in [MatchError, FunctionClauseError] -> {:error, e.__struct__}
          end

        assert match?({:error, _}, result),
               "Expected MatchError or FunctionClauseError but got #{inspect(result)}"
      end
    end

    test "partial correlation_id fails" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])

        partial = <<0, 0, 0>>

        assert_raise MatchError, fn ->
          response_module.deserialize(partial)
        end
      end
    end
  end

  # ============================================
  # Helper Response Builders
  # ============================================

  describe "custom response builders" do
    test "minimal_response creates valid empty response" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])
        minimal = DescribeConfigsFactory.minimal_response(version)

        {response, <<>>} = response_module.deserialize(minimal)
        assert response.resources == []
      end
    end

    test "error_response creates valid error response" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])

        error_binary =
          DescribeConfigsFactory.error_response(version,
            error_code: 40,
            resource_name: "bad-topic"
          )

        {response, <<>>} = response_module.deserialize(error_binary)
        [resource] = response.resources
        assert resource.error_code == 40
        assert resource.error_message == "Invalid config"
        assert resource.resource_name == "bad-topic"
      end
    end

    test "custom response with multiple resources" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])

        config_entry =
          if version == 0 do
            %{
              config_name: "key1",
              config_value: "value1",
              read_only: 0,
              is_default: 0,
              is_sensitive: 0
            }
          else
            %{
              config_name: "key1",
              config_value: "value1",
              read_only: 0,
              config_source: 5,
              is_sensitive: 0,
              config_synonyms: []
            }
          end

        multi_resource_binary =
          DescribeConfigsFactory.response_binary(version,
            resources: [
              %{
                error_code: 0,
                error_message: nil,
                resource_type: 2,
                resource_name: "topic-1",
                config_entries: [config_entry]
              },
              %{
                error_code: 0,
                error_message: nil,
                resource_type: 2,
                resource_name: "topic-2",
                config_entries: []
              }
            ]
          )

        {response, <<>>} = response_module.deserialize(multi_resource_binary)
        assert length(response.resources) == 2
        assert Enum.map(response.resources, & &1.resource_name) == ["topic-1", "topic-2"]
      end
    end
  end

  # ============================================
  # Protocol Invariants
  # ============================================

  describe "protocol invariants" do
    test "correlation_id is preserved in response" do
      for version <- 0..2 do
        response_module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Response])
        {response_binary, _expected_struct} = DescribeConfigsFactory.response_data(version)

        {response, _rest} = response_module.deserialize(response_binary)
        assert response.correlation_id == version
      end
    end

    test "throttle_time_ms is always present in response" do
      for version <- 0..2 do
        {_binary, response} = DescribeConfigsFactory.response_data(version)
        assert Map.has_key?(response, :throttle_time_ms)
        assert is_integer(response.throttle_time_ms)
      end
    end

    test "resources is always a list" do
      for version <- 0..2 do
        {_binary, response} = DescribeConfigsFactory.response_data(version)
        assert is_list(response.resources)
      end
    end

    test "config_entries is always a list" do
      for version <- 0..2 do
        {_binary, response} = DescribeConfigsFactory.response_data(version)

        for resource <- response.resources do
          assert is_list(resource.config_entries)
        end
      end
    end
  end
end
