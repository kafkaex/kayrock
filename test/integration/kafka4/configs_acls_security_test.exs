defmodule Kayrock.Kafka4.ConfigsAclsSecurityTest do
  @moduledoc """
  Kafka 4.4.0 integration tests for the configs / ACLs / quotas / security
  versions generated from the JSON specs:

    * DescribeConfigs v3-v4, AlterConfigs v2
    * DescribeAcls / CreateAcls / DeleteAcls v2-v3
    * SaslAuthenticate v2 (and v0-v1, which the version-compatibility suite
      has to skip)
    * CreateDelegationToken v3, RenewDelegationToken v2,
      ExpireDelegationToken v2, DescribeDelegationToken v2-v3
    * DescribeClientQuotas / AlterClientQuotas v0-v1
    * DescribeUserScramCredentials / AlterUserScramCredentials v0

  The broker's client listener (`PLAINTEXT`, the only mapped port) is
  switched to `SASL_PLAINTEXT` with the PLAIN mechanism, the KRaft
  `StandardAuthorizer` is enabled and a delegation-token secret is set, so the
  ACL and delegation-token APIs succeed instead of returning
  SECURITY_DISABLED / DELEGATION_TOKEN_AUTH_DISABLED. `User:admin` (and
  `User:ANONYMOUS`, the principal of the plaintext inter-broker and
  controller listeners) are super users.

  Every request goes over a raw `Kayrock.BrokerConnection` that has done
  SaslHandshake v1 + SaslAuthenticate, and every response must deserialize
  with no leftover bytes.
  """
  use Kayrock.Kafka4Case,
    container:
      Kayrock.Kafka4Container.new()
      |> Kayrock.Kafka4Container.with_broker_config(%{
        "listener.security.protocol.map" =>
          "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "sasl.enabled.mechanisms" => "PLAIN",
        "listener.name.plaintext.plain.sasl.jaas.config" =>
          "org.apache.kafka.common.security.plain.PlainLoginModule required " <>
            ~s(user_admin="admin-secret";),
        "authorizer.class.name" => "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "super.users" => "User:admin;User:ANONYMOUS",
        "delegation.token.secret.key" => "kayrock-delegation-token-secret"
      })

  import Kayrock.TestSupport, only: [unique_string: 0]

  @admin_auth <<0, "admin", 0, "admin-secret">>

  # ResourceType / PatternType / AclOperation / AclPermissionType values
  @topic_resource 2
  @broker_resource 4
  @literal 3
  @any 1
  @read 3
  @allow 3

  # ScramMechanism
  @scram_sha_256 1

  setup %{kafka: kafka} do
    conn = authenticated_conn(kafka, Kayrock.SaslAuthenticate.max_vsn())
    on_exit(fn -> Process.exit(conn, :kill) end)
    {:ok, conn: conn}
  end

  describe "SaslAuthenticate" do
    for version <- 0..2 do
      test "v#{version} authenticates with PLAIN", %{kafka: kafka} do
        conn = authenticated_conn(kafka, unquote(version))
        Process.exit(conn, :kill)
      end
    end

    test "v2 decodes an authentication failure", %{kafka: kafka} do
      conn = raw_conn(kafka)
      :ok = sasl_handshake(conn)

      response =
        call!(conn, %Kayrock.SaslAuthenticate.V2.Request{
          auth_bytes: <<0, "admin", 0, "wrong">>,
          tagged_fields: []
        })

      # SASL_AUTHENTICATION_FAILED
      assert response.error_code == 58
      assert is_binary(response.error_message)
    end
  end

  describe "DescribeConfigs / AlterConfigs" do
    for version <- 3..4 do
      test "DescribeConfigs v#{version} describes a topic and the broker", %{conn: conn} do
        version = unquote(version)
        topic = create_topic(conn)

        request =
          version
          |> Kayrock.DescribeConfigs.get_request_struct()
          |> struct(
            resources: [
              resource(version, %{
                resource_type: @topic_resource,
                resource_name: topic,
                configuration_keys: ["cleanup.policy"]
              }),
              resource(version, %{
                resource_type: @broker_resource,
                resource_name: "1",
                configuration_keys: nil
              })
            ],
            include_synonyms: true,
            include_documentation: true
          )

        %{results: [topic_result, broker_result]} = call!(conn, request)

        assert %{error_code: 0, resource_name: ^topic, configs: [config]} = topic_result
        assert config.name == "cleanup.policy"
        assert config.value == "delete"
        # STRING / LIST config type and documentation (v3+)
        assert config.config_type > 0
        assert is_binary(config.documentation)

        assert %{error_code: 0, resource_name: "1", configs: configs} = broker_result
        assert Enum.any?(configs, &(&1.name == "log.dirs"))
      end
    end

    test "AlterConfigs v2 sets a topic config", %{conn: conn} do
      topic = create_topic(conn)

      request = %Kayrock.AlterConfigs.V2.Request{
        resources: [
          %{
            resource_type: @topic_resource,
            resource_name: topic,
            configs: [%{name: "retention.ms", value: "123456", tagged_fields: []}],
            tagged_fields: []
          }
        ],
        validate_only: false,
        tagged_fields: []
      }

      assert %{responses: [%{error_code: 0, resource_name: ^topic}]} = call!(conn, request)

      %{results: [%{configs: [config]}]} =
        call!(conn, %Kayrock.DescribeConfigs.V4.Request{
          resources: [
            %{
              resource_type: @topic_resource,
              resource_name: topic,
              configuration_keys: ["retention.ms"],
              tagged_fields: []
            }
          ],
          include_synonyms: false,
          include_documentation: false,
          tagged_fields: []
        })

      assert config.value == "123456"
    end
  end

  describe "ACLs" do
    for version <- 2..3 do
      test "CreateAcls / DescribeAcls / DeleteAcls v#{version}", %{conn: conn} do
        version = unquote(version)
        topic = "acl-" <> unique_string()
        principal = "User:alice"

        create =
          version
          |> Kayrock.CreateAcls.get_request_struct()
          |> struct(
            creations: [
              %{
                resource_type: @topic_resource,
                resource_name: topic,
                resource_pattern_type: @literal,
                principal: principal,
                host: "*",
                operation: @read,
                permission_type: @allow,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          )

        assert %{results: [%{error_code: 0}]} = call!(conn, create)

        describe =
          version
          |> Kayrock.DescribeAcls.get_request_struct()
          |> struct(
            resource_type_filter: @topic_resource,
            resource_name_filter: topic,
            pattern_type_filter: @literal,
            principal_filter: nil,
            host_filter: nil,
            operation: @any,
            permission_type: @any,
            tagged_fields: []
          )

        # ACL creation is applied through the metadata log; poll briefly
        resources =
          poll(fn ->
            case call!(conn, describe) do
              %{error_code: 0, resources: [_ | _] = resources} -> resources
              _ -> nil
            end
          end)

        assert [
                 %{
                   resource_type: @topic_resource,
                   resource_name: ^topic,
                   pattern_type: @literal,
                   acls: [
                     %{
                       principal: ^principal,
                       host: "*",
                       operation: @read,
                       permission_type: @allow
                     }
                   ]
                 }
               ] = resources

        delete =
          version
          |> Kayrock.DeleteAcls.get_request_struct()
          |> struct(
            filters: [
              %{
                resource_type_filter: @topic_resource,
                resource_name_filter: topic,
                pattern_type_filter: @literal,
                principal_filter: principal,
                host_filter: nil,
                operation: @any,
                permission_type: @any,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          )

        assert %{
                 filter_results: [
                   %{
                     error_code: 0,
                     matching_acls: [
                       %{error_code: 0, resource_name: ^topic, principal: ^principal}
                     ]
                   }
                 ]
               } = call!(conn, delete)
      end
    end
  end

  describe "delegation tokens" do
    test "Create v3 / Describe v2-v3 / Renew v2 / Expire v2", %{conn: conn} do
      created =
        call!(conn, %Kayrock.CreateDelegationToken.V3.Request{
          owner_principal_type: nil,
          owner_principal_name: nil,
          renewers: [],
          max_lifetime_ms: -1,
          tagged_fields: []
        })

      assert created.error_code == 0
      assert created.principal_type == "User"
      assert created.principal_name == "admin"
      assert created.token_requester_principal_type == "User"
      assert created.token_requester_principal_name == "admin"
      assert is_binary(created.token_id) and created.token_id != ""
      assert byte_size(created.hmac) > 0

      token_id = created.token_id

      for version <- 2..3 do
        request =
          struct(Kayrock.DescribeDelegationToken.get_request_struct(version),
            owners: nil,
            tagged_fields: []
          )

        # tokens are applied through the metadata log; poll briefly
        token = find_token(conn, request, token_id)
        assert token.hmac == created.hmac
        assert token.principal_name == "admin"

        if version >= 3 do
          assert token.token_requester_principal_name == "admin"
        end
      end

      renewed =
        call!(conn, %Kayrock.RenewDelegationToken.V2.Request{
          hmac: created.hmac,
          renew_period_ms: 60_000,
          tagged_fields: []
        })

      assert renewed.error_code == 0
      assert renewed.expiry_timestamp_ms > 0

      expired =
        call!(conn, %Kayrock.ExpireDelegationToken.V2.Request{
          hmac: created.hmac,
          expiry_time_period_ms: -1,
          tagged_fields: []
        })

      assert expired.error_code == 0
    end

    test "CreateDelegationToken v3 for another owner", %{conn: conn} do
      created =
        call!(conn, %Kayrock.CreateDelegationToken.V3.Request{
          owner_principal_type: "User",
          owner_principal_name: "bob",
          renewers: [%{principal_type: "User", principal_name: "carol", tagged_fields: []}],
          max_lifetime_ms: -1,
          tagged_fields: []
        })

      assert created.error_code == 0
      assert created.principal_name == "bob"
      assert created.token_requester_principal_name == "admin"

      describe = %Kayrock.DescribeDelegationToken.V3.Request{
        owners: [%{principal_type: "User", principal_name: "bob", tagged_fields: []}],
        tagged_fields: []
      }

      assert %{
               principal_name: "bob",
               renewers: [%{principal_type: "User", principal_name: "carol"}]
             } =
               find_token(conn, describe, created.token_id)
    end
  end

  describe "client quotas" do
    for version <- 0..1 do
      test "AlterClientQuotas / DescribeClientQuotas v#{version}", %{conn: conn} do
        version = unquote(version)
        user = "quota-" <> unique_string()

        alter =
          version
          |> Kayrock.AlterClientQuotas.get_request_struct()
          |> struct(
            entries: [
              flex(version, %{
                entity: [flex(version, %{entity_type: "user", entity_name: user})],
                ops: [
                  flex(version, %{key: "producer_byte_rate", value: 1024.0, remove: false}),
                  flex(version, %{key: "request_percentage", value: 12.5, remove: false})
                ]
              })
            ],
            validate_only: false
          )

        assert %{
                 entries: [%{error_code: 0, entity: [%{entity_type: "user", entity_name: ^user}]}]
               } =
                 call!(conn, alter)

        describe =
          version
          |> Kayrock.DescribeClientQuotas.get_request_struct()
          |> struct(
            components: [flex(version, %{entity_type: "user", match_type: 0, match: user})],
            strict: true
          )

        entries =
          poll(fn ->
            case call!(conn, describe) do
              %{error_code: 0, entries: [_ | _] = entries} -> entries
              _ -> nil
            end
          end)

        assert [%{entity: [%{entity_type: "user", entity_name: ^user}], values: values}] = entries

        assert values |> Enum.map(&{&1.key, &1.value}) |> Enum.sort() == [
                 {"producer_byte_rate", 1024.0},
                 {"request_percentage", 12.5}
               ]
      end
    end
  end

  describe "SCRAM credentials" do
    test "AlterUserScramCredentials / DescribeUserScramCredentials v0", %{conn: conn} do
      user = "scram-" <> unique_string()

      upsert = %Kayrock.AlterUserScramCredentials.V0.Request{
        deletions: [],
        upsertions: [
          %{
            name: user,
            mechanism: @scram_sha_256,
            iterations: 4096,
            salt: :crypto.strong_rand_bytes(16),
            salted_password: :crypto.strong_rand_bytes(32),
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert %{results: [%{user: ^user, error_code: 0}]} = call!(conn, upsert)

      describe = %Kayrock.DescribeUserScramCredentials.V0.Request{
        users: [%{name: user, tagged_fields: []}],
        tagged_fields: []
      }

      result =
        poll(fn ->
          case call!(conn, describe) do
            %{error_code: 0, results: [%{error_code: 0} = result]} -> result
            _ -> nil
          end
        end)

      assert %{user: ^user, credential_infos: [%{mechanism: @scram_sha_256, iterations: 4096}]} =
               result

      # null users = describe all
      assert %{error_code: 0, results: all} =
               call!(conn, %{describe | users: nil})

      assert Enum.any?(all, &(&1.user == user))

      delete = %Kayrock.AlterUserScramCredentials.V0.Request{
        deletions: [%{name: user, mechanism: @scram_sha_256, tagged_fields: []}],
        upsertions: [],
        tagged_fields: []
      }

      assert %{results: [%{user: ^user, error_code: 0}]} = call!(conn, delete)
    end

    test "DescribeUserScramCredentials v0 decodes RESOURCE_NOT_FOUND", %{conn: conn} do
      %{error_code: 0, results: [result]} =
        call!(conn, %Kayrock.DescribeUserScramCredentials.V0.Request{
          users: [%{name: "no-such-user-" <> unique_string(), tagged_fields: []}],
          tagged_fields: []
        })

      # RESOURCE_NOT_FOUND
      assert result.error_code == 91
      assert is_binary(result.error_message)
    end
  end

  # -- helpers ---------------------------------------------------------------

  defp raw_conn(kafka) do
    {:ok, conn} = connect_broker(kafka)
    Process.unlink(conn)
    conn
  end

  defp authenticated_conn(kafka, sasl_authenticate_version) do
    conn = raw_conn(kafka)
    :ok = sasl_handshake(conn)

    request =
      sasl_authenticate_version
      |> Kayrock.SaslAuthenticate.get_request_struct()
      |> Map.put(:auth_bytes, @admin_auth)

    response = call!(conn, request)
    assert response.error_code == 0, inspect(response)

    if sasl_authenticate_version >= 1 do
      assert response.session_lifetime_ms == 0
    end

    conn
  end

  defp sasl_handshake(conn) do
    %{error_code: 0, mechanisms: mechanisms} =
      call!(conn, %Kayrock.SaslHandshake.V1.Request{mechanism: "PLAIN"})

    assert "PLAIN" in mechanisms
    :ok
  end

  # Sends `request`, asserts the response is complete (no leftover bytes)
  # and carries the request's correlation id.
  defp call!(conn, request) do
    correlation_id = rem(System.unique_integer([:positive]), 1_000_000)
    request = %{request | correlation_id: correlation_id, client_id: "kayrock-kafka4"}

    :ok = Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request))
    {:ok, data} = Kayrock.BrokerConnection.recv(conn, 10_000)
    {response, rest} = Kayrock.Request.response_deserializer(request).(data)

    assert rest == <<>>, "#{inspect(request.__struct__)}: #{byte_size(rest)} leftover bytes"
    assert response.correlation_id == correlation_id
    response
  end

  defp create_topic(conn) do
    name = unique_string()

    %{topics: [%{name: ^name, error_code: 0}]} =
      call!(conn, %Kayrock.CreateTopics.V5.Request{
        topics: [
          %{
            name: name,
            num_partitions: 1,
            replication_factor: 1,
            assignments: [],
            configs: [],
            tagged_fields: []
          }
        ],
        timeout_ms: 10_000,
        validate_only: false,
        tagged_fields: []
      })

    name
  end

  defp find_token(conn, describe_request, token_id) do
    poll(fn ->
      %{error_code: 0, tokens: tokens} = call!(conn, describe_request)
      Enum.find(tokens, &(&1.token_id == token_id))
    end)
  end

  defp resource(4, map), do: Map.put(map, :tagged_fields, [])
  defp resource(_version, map), do: map

  # v1+ of the client quota APIs are flexible
  defp flex(0, map), do: map
  defp flex(_version, map), do: Map.put(map, :tagged_fields, [])

  defp poll(fun, retries \\ 50) do
    case fun.() do
      nil when retries > 0 ->
        Process.sleep(100)
        poll(fun, retries - 1)

      nil ->
        flunk("condition not met")

      value ->
        value
    end
  end
end
