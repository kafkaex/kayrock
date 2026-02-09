defmodule Kayrock.ErrorCode do
  @moduledoc """
  Utility for converting Kafka error codes to/from atom names

  Note the actual error code values are determined by `:kpro_schema.c/1` for codes -1 to 71.
  Extended error codes (72-119) are defined locally to support newer Kafka versions.

  See https://kafka.apache.org/protocol#protocol_error_codes
  """

  defmodule InvalidErrorCodeException do
    @moduledoc """
    Raised if `Kayrock.ErrorCode.code_to_atom!/1` is called with a code that
    does not correspond to a known Kafka error code.
    """
    defexception [:message]

    @impl true
    def exception(code) do
      %__MODULE__{message: "Invalid error code: #{inspect(code)}"}
    end
  end

  defmodule InvalidAtomException do
    @moduledoc """
    Raised if `Kayrock.ErrorCode.atom_to_code!/1` is called with an atom that
    does not correspond to a known Kafka error code.
    """
    defexception [:message]

    @impl true
    def exception(atom) do
      %__MODULE__{message: "Invalid error atom: #{inspect(atom)}"}
    end
  end

  alias Kayrock.KafkaSchemaMetadata

  # Extended error codes not in kafka_protocol (72-119)
  # These were added in Kafka 2.1+ versions
  # See: https://kafka.apache.org/protocol#protocol_error_codes
  @extended_codes %{
    72 => :listener_not_found,
    73 => :topic_deletion_disabled,
    74 => :fenced_leader_epoch,
    75 => :unknown_leader_epoch,
    76 => :unsupported_compression_type,
    77 => :stale_broker_epoch,
    78 => :offset_not_available,
    79 => :member_id_required,
    80 => :preferred_leader_not_available,
    81 => :group_max_size_reached,
    82 => :fenced_instance_id,
    83 => :eligible_leaders_not_available,
    84 => :election_not_needed,
    85 => :no_reassignment_in_progress,
    86 => :group_subscribed_to_topic,
    87 => :invalid_record,
    88 => :unstable_offset_commit,
    89 => :throttling_quota_exceeded,
    90 => :producer_fenced,
    91 => :resource_not_found,
    92 => :duplicate_resource,
    93 => :unacceptable_credential,
    94 => :inconsistent_voter_set,
    95 => :invalid_update_version,
    96 => :feature_update_failed,
    97 => :principal_deserialization_failure,
    98 => :snapshot_not_found,
    99 => :position_out_of_range,
    100 => :unknown_topic_id,
    101 => :duplicate_broker_registration,
    102 => :broker_id_not_registered,
    103 => :inconsistent_topic_id,
    104 => :inconsistent_cluster_id,
    105 => :transactional_id_not_found,
    106 => :fetch_session_topic_id_error,
    107 => :ineligible_replica,
    108 => :new_leader_elected,
    109 => :offset_moved_to_tiered_storage,
    110 => :fenced_member_epoch,
    111 => :unreleased_instance_id,
    112 => :unsupported_assignor,
    113 => :stale_member_epoch,
    114 => :mismatched_endpoint_type,
    115 => :unsupported_endpoint_type,
    116 => :unknown_controller_id,
    117 => :unknown_subscription_id,
    118 => :telemetry_too_large,
    119 => :invalid_registration
  }

  # Reverse mapping for extended codes
  @extended_atoms Map.new(@extended_codes, fn {code, atom} -> {atom, code} end)

  # Base codes from KafkaSchemaMetadata
  @base_codes -1..71

  case Code.ensure_compiled(KafkaSchemaMetadata) do
    {:module, _} ->
      base_atom_to_code =
        Enum.into(@base_codes, %{}, fn c ->
          {KafkaSchemaMetadata.error_code_to_error(c), c}
        end)

      @atom_to_code Map.merge(base_atom_to_code, @extended_atoms)

    _ ->
      # KafkaSchemaMetadata isn't available at compile time (e.g. before generation).
      # Use only extended codes; base codes will work once the module exists.
      @atom_to_code @extended_atoms
  end

  @typedoc """
  A numeric Kafka error code
  """
  @type error_code :: integer

  @typedoc """
  An erlang/elixir atom representation of an error code
  """
  @type error_atom :: atom

  require Logger

  @doc """
  Converts an error code to an atom

  An unknown code results in a return of `:unknown`
  """
  @spec code_to_atom(error_code) :: error_atom
  def code_to_atom(code) when code in @base_codes do
    KafkaSchemaMetadata.error_code_to_error(code)
  end

  def code_to_atom(code) when is_map_key(@extended_codes, code) do
    Map.fetch!(@extended_codes, code)
  end

  def code_to_atom(code) do
    Logger.warning("Unknown error code #{inspect(code)}")
    :unknown
  end

  @doc """
  Converts an error code to an atom, raising an
  `Kayrock.ErrorCode.InvalidErrorCodeException` if an unknown error code is supplied
  """
  @spec code_to_atom!(error_code) :: error_atom
  def code_to_atom!(code) do
    case code_to_atom(code) do
      :unknown -> raise InvalidErrorCodeException, code
      v -> v
    end
  end

  @doc """
  Converts an atom to a numeric error code

  Returns nil if an unknown atom is supplied
  """
  @spec atom_to_code(error_atom) :: error_code | nil
  def atom_to_code(atom) do
    Map.get(@atom_to_code, atom)
  end

  @doc """
  Converts an atom to a numeric error code, raising an
  `Kayrock.ErrorCode.InvalidAtomException` if an unknown atom is supplied
  """
  @spec atom_to_code!(error_atom) :: error_code
  def atom_to_code!(atom) do
    case atom_to_code(atom) do
      nil -> raise InvalidAtomException, atom
      v -> v
    end
  end

  ######################################################################
  # convenience functions
  @spec unknown_topic() :: error_code
  def unknown_topic, do: atom_to_code!(:unknown_topic_or_partition)
end
