# Configure ExUnit to exclude integration/chaos by default
ExUnit.configure(exclude: [:integration, :chaos])
ExUnit.start()

# Start testcontainers GenServer AFTER ExUnit.start() (if available and configured)
# Set ENABLE_TESTCONTAINERS=true to enable integration/chaos tests
# Uses start/1 (not start_link/1) so GenServer survives the caller process exit
if System.get_env("ENABLE_TESTCONTAINERS") == "true" and Code.ensure_loaded?(Testcontainers) do
  # Ensure required applications are started
  {:ok, _} = Application.ensure_all_started(:hackney)

  # Start the Testcontainers GenServer (unlinked, so it survives)
  case Testcontainers.start() do
    {:ok, _pid} ->
      :ok

    {:error, {:already_started, _pid}} ->
      :ok

    {:error, reason} ->
      IO.puts(:stderr, "Warning: Failed to start Testcontainers: #{inspect(reason)}")
      IO.puts(:stderr, "Integration and chaos tests will fail.")
  end
end
