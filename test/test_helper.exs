# Configure ExUnit to exclude integration/chaos by default
ExUnit.configure(exclude: [:integration, :chaos])
ExUnit.start()

# Check if we need Testcontainers (integration or chaos tests requested)
# Detects --only integration, --only chaos, --include integration, --include chaos
needs_testcontainers? =
  System.get_env("ENABLE_TESTCONTAINERS") == "true" or
    Enum.any?(System.argv(), fn arg ->
      arg in ["--only", "--include"] or
        String.contains?(arg, "integration") or
        String.contains?(arg, "chaos")
    end) or
    System.argv()
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.any?(fn
      ["--only", tag] -> tag in ["integration", "chaos"]
      ["--include", tag] -> tag in ["integration", "chaos"]
      _ -> false
    end)

# Start testcontainers GenServer AFTER ExUnit.start() (if available and needed)
# Uses start/1 (not start_link/1) so GenServer survives the caller process exit
if needs_testcontainers? and Code.ensure_loaded?(Testcontainers) do
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
