defmodule Kayrock.Client.CompressionTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "with compression" do
    setup do
      on_exit(fn ->
        Application.put_env(:kayrock, :snappy_module, :snappy)
      end)

      :ok
    end

    test "gzip produce works", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)
      topic_name = create_topic(client_pid)

      timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      record_batch = %Kayrock.RecordBatch{
        attributes: 1,
        records: [
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: "foo",
            headers: [],
            timestamp: timestamp,
            attributes: 1
          }
        ]
      }

      {:ok, resp} = Kayrock.produce(client_pid, record_batch, topic_name, 0)

      partition_resp =
        resp.responses |> List.first() |> Map.get(:partition_responses) |> List.first()

      partition = partition_resp |> Map.get(:partition)
      offset = partition_resp |> Map.get(:base_offset)

      {:ok, resp} = Kayrock.fetch(client_pid, topic_name, partition, offset)

      assert_record_batch(resp, %Kayrock.RecordBatch.Record{key: "1", value: "foo"})
    end

    test "using snappyer produce works", %{kafka: kafka} do
      Application.put_env(:kayrock, :snappy_module, :snappyer)
      {:ok, client_pid} = build_client(kafka)
      topic_name = create_topic(client_pid)

      timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      record_batch = %Kayrock.RecordBatch{
        attributes: 2,
        records: [
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: "foo",
            headers: [],
            timestamp: timestamp,
            attributes: 1
          }
        ]
      }

      {:ok, resp} = Kayrock.produce(client_pid, record_batch, topic_name, 0)

      partition_resp =
        resp.responses |> List.first() |> Map.get(:partition_responses) |> List.first()

      partition = partition_resp |> Map.get(:partition)
      offset = partition_resp |> Map.get(:base_offset)

      {:ok, resp} = Kayrock.fetch(client_pid, topic_name, partition, offset)

      assert_record_batch(resp, %Kayrock.RecordBatch.Record{key: "1", value: "foo"})
    end

    test "using snappy-erlang-nif produce works", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)
      topic_name = create_topic(client_pid)

      timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      record_batch = %Kayrock.RecordBatch{
        attributes: 2,
        records: [
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: "foo",
            headers: [],
            timestamp: timestamp,
            attributes: 1
          }
        ]
      }

      {:ok, resp} = Kayrock.produce(client_pid, record_batch, topic_name, 0)

      partition_resp =
        resp.responses |> List.first() |> Map.get(:partition_responses) |> List.first()

      partition = partition_resp |> Map.get(:partition)
      offset = partition_resp |> Map.get(:base_offset)

      {:ok, resp} = Kayrock.fetch(client_pid, topic_name, partition, offset)

      assert_record_batch(resp, %Kayrock.RecordBatch.Record{key: "1", value: "foo"})
    end
  end

  defp build_client(kafka) do
    uris = [{"localhost", Container.mapped_port(kafka, 9092)}]
    Kayrock.Client.start_link(uris)
  end

  defp create_topic(client_pid) do
    topic_name = unique_string()
    create_request = create_topic_request(topic_name, 5)
    {:ok, _} = Kayrock.client_call(client_pid, create_request, :controller)
    topic_name
  end

  defp assert_record_batch(response, record) do
    responses =
      response.responses |> List.first() |> Map.get(:partition_responses) |> List.first()

    record_set = responses |> Map.get(:record_set) |> List.first()
    [received_record] = record_set.records

    assert received_record.key == record.key
    assert received_record.value == record.value
  end
end
