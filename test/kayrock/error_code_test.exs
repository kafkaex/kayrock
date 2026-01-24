defmodule Kayrock.ErrorCodeTest do
  @moduledoc """
  Tests for Kayrock.ErrorCode module - error code conversions.
  """
  use ExUnit.Case, async: true

  alias Kayrock.ErrorCode
  alias Kayrock.ErrorCode.InvalidAtomException
  alias Kayrock.ErrorCode.InvalidErrorCodeException

  describe "code_to_atom/1" do
    test "converts 0 to :no_error" do
      assert :no_error = ErrorCode.code_to_atom(0)
    end

    test "converts -1 to :unknown_server_error" do
      assert :unknown_server_error = ErrorCode.code_to_atom(-1)
    end

    test "converts common error codes" do
      # A few well-known error codes
      assert :offset_out_of_range = ErrorCode.code_to_atom(1)
      assert :corrupt_message = ErrorCode.code_to_atom(2)
      assert :unknown_topic_or_partition = ErrorCode.code_to_atom(3)
      assert :leader_not_available = ErrorCode.code_to_atom(5)
      assert :not_leader_for_partition = ErrorCode.code_to_atom(6)
      assert :request_timed_out = ErrorCode.code_to_atom(7)
    end

    test "returns :unknown for code outside known range" do
      import ExUnit.CaptureLog

      assert capture_log(fn ->
               assert :unknown = ErrorCode.code_to_atom(9999)
             end) =~ "Unknown error code"

      assert capture_log(fn ->
               assert :unknown = ErrorCode.code_to_atom(-100)
             end) =~ "Unknown error code"
    end
  end

  describe "code_to_atom!/1" do
    test "converts valid codes" do
      assert :no_error = ErrorCode.code_to_atom!(0)
      assert :unknown_server_error = ErrorCode.code_to_atom!(-1)
    end

    test "raises InvalidErrorCodeException for unknown codes" do
      import ExUnit.CaptureLog

      capture_log(fn ->
        assert_raise InvalidErrorCodeException, ~r/Invalid error code: 9999/, fn ->
          ErrorCode.code_to_atom!(9999)
        end
      end)
    end
  end

  describe "atom_to_code/1" do
    test "converts :no_error to 0" do
      assert 0 = ErrorCode.atom_to_code(:no_error)
    end

    test "converts :unknown_server_error to -1" do
      assert -1 = ErrorCode.atom_to_code(:unknown_server_error)
    end

    test "converts common error atoms" do
      assert 1 = ErrorCode.atom_to_code(:offset_out_of_range)
      assert 2 = ErrorCode.atom_to_code(:corrupt_message)
      assert 3 = ErrorCode.atom_to_code(:unknown_topic_or_partition)
      assert 5 = ErrorCode.atom_to_code(:leader_not_available)
      assert 6 = ErrorCode.atom_to_code(:not_leader_for_partition)
      assert 7 = ErrorCode.atom_to_code(:request_timed_out)
    end

    test "returns nil for unknown atoms" do
      assert nil == ErrorCode.atom_to_code(:not_a_real_error)
      assert nil == ErrorCode.atom_to_code(:made_up_error)
    end
  end

  describe "atom_to_code!/1" do
    test "converts valid atoms" do
      assert 0 = ErrorCode.atom_to_code!(:no_error)
      assert -1 = ErrorCode.atom_to_code!(:unknown_server_error)
    end

    test "raises InvalidAtomException for unknown atoms" do
      assert_raise InvalidAtomException, ~r/Invalid error atom: :not_real/, fn ->
        ErrorCode.atom_to_code!(:not_real)
      end
    end
  end

  describe "convenience functions" do
    test "unknown_topic/0 returns correct code" do
      assert 3 = ErrorCode.unknown_topic()
    end
  end

  describe "InvalidErrorCodeException" do
    test "creates exception with message" do
      exception = InvalidErrorCodeException.exception(123)
      assert exception.message == "Invalid error code: 123"
    end
  end

  describe "InvalidAtomException" do
    test "creates exception with message" do
      exception = InvalidAtomException.exception(:bad_atom)
      assert exception.message == "Invalid error atom: :bad_atom"
    end
  end

  describe "round-trip conversions" do
    test "code -> atom -> code is consistent" do
      for code <- [0, -1, 1, 2, 3, 5, 6, 7, 10, 15, 20, 25, 30, 35, 40, 50, 60, 71] do
        atom = ErrorCode.code_to_atom(code)
        assert atom != :unknown, "Code #{code} should be known"
        assert ^code = ErrorCode.atom_to_code(atom)
      end
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "edge cases" do
    test "all consumer group error codes convert" do
      # Consumer group specific errors
      consumer_errors = [
        {14, :coordinator_load_in_progress},
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {22, :illegal_generation},
        {25, :unknown_member_id},
        {26, :invalid_session_timeout},
        {27, :rebalance_in_progress},
        {28, :invalid_commit_offset_size}
      ]

      for {code, expected_atom} <- consumer_errors do
        assert ^expected_atom = ErrorCode.code_to_atom(code)
        assert ^code = ErrorCode.atom_to_code(expected_atom)
      end
    end

    test "all transaction error codes convert" do
      transaction_errors = [
        {47, :invalid_producer_epoch},
        {48, :invalid_txn_state},
        {49, :invalid_producer_id_mapping}
      ]

      for {code, expected_atom} <- transaction_errors do
        assert ^expected_atom = ErrorCode.code_to_atom(code)
        assert ^code = ErrorCode.atom_to_code(expected_atom)
      end
    end

    test "boundary error codes (min and max)" do
      # -1 is lowest
      assert :unknown_server_error = ErrorCode.code_to_atom(-1)

      # 119 is highest defined in the codebase (extended codes)
      assert :invalid_registration = ErrorCode.code_to_atom(119)
    end

    test "consecutive calls return same values" do
      # Test caching/consistency
      result1 = ErrorCode.code_to_atom(25)
      result2 = ErrorCode.code_to_atom(25)
      assert result1 == result2
      assert result1 == :unknown_member_id
    end

    test "all broker error codes convert" do
      broker_errors = [
        {5, :leader_not_available},
        {6, :not_leader_for_partition},
        {7, :request_timed_out},
        {8, :broker_not_available},
        {9, :replica_not_available}
      ]

      for {code, expected_atom} <- broker_errors do
        assert ^expected_atom = ErrorCode.code_to_atom(code)
      end
    end

    test "extended error codes (72-89) convert correctly" do
      extended_errors_72_89 = [
        {72, :listener_not_found},
        {73, :topic_deletion_disabled},
        {74, :fenced_leader_epoch},
        {75, :unknown_leader_epoch},
        {76, :unsupported_compression_type},
        {77, :stale_broker_epoch},
        {78, :offset_not_available},
        {79, :member_id_required},
        {80, :preferred_leader_not_available},
        {81, :group_max_size_reached},
        {82, :fenced_instance_id},
        {83, :eligible_leaders_not_available},
        {84, :election_not_needed},
        {85, :no_reassignment_in_progress},
        {86, :group_subscribed_to_topic},
        {87, :invalid_record},
        {88, :unstable_offset_commit},
        {89, :throttling_quota_exceeded}
      ]

      for {code, expected_atom} <- extended_errors_72_89 do
        assert ^expected_atom = ErrorCode.code_to_atom(code)
        assert ^code = ErrorCode.atom_to_code(expected_atom)
      end
    end

    test "extended error codes (90-109) convert correctly" do
      extended_errors_90_109 = [
        {90, :producer_fenced},
        {91, :resource_not_found},
        {92, :duplicate_resource},
        {93, :unacceptable_credential},
        {94, :inconsistent_voter_set},
        {95, :invalid_update_version},
        {96, :feature_update_failed},
        {97, :principal_deserialization_failure},
        {98, :snapshot_not_found},
        {99, :position_out_of_range},
        {100, :unknown_topic_id},
        {101, :duplicate_broker_registration},
        {102, :broker_id_not_registered},
        {103, :inconsistent_topic_id},
        {104, :inconsistent_cluster_id},
        {105, :transactional_id_not_found},
        {106, :fetch_session_topic_id_error},
        {107, :ineligible_replica},
        {108, :new_leader_elected},
        {109, :offset_moved_to_tiered_storage}
      ]

      for {code, expected_atom} <- extended_errors_90_109 do
        assert ^expected_atom = ErrorCode.code_to_atom(code)
        assert ^code = ErrorCode.atom_to_code(expected_atom)
      end
    end

    test "extended error codes (110-119) convert correctly" do
      extended_errors_110_119 = [
        {110, :fenced_member_epoch},
        {111, :unreleased_instance_id},
        {112, :unsupported_assignor},
        {113, :stale_member_epoch},
        {114, :mismatched_endpoint_type},
        {115, :unsupported_endpoint_type},
        {116, :unknown_controller_id},
        {117, :unknown_subscription_id},
        {118, :telemetry_too_large},
        {119, :invalid_registration}
      ]

      for {code, expected_atom} <- extended_errors_110_119 do
        assert ^expected_atom = ErrorCode.code_to_atom(code)
        assert ^code = ErrorCode.atom_to_code(expected_atom)
      end
    end

    test "codes beyond 119 return :unknown" do
      import ExUnit.CaptureLog

      assert capture_log(fn ->
               assert :unknown = ErrorCode.code_to_atom(120)
             end) =~ "Unknown error code"

      assert capture_log(fn ->
               assert :unknown = ErrorCode.code_to_atom(150)
             end) =~ "Unknown error code"
    end
  end
end
