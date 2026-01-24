# Start testcontainers GenServer before ExUnit (if available and configured)
# Set ENABLE_TESTCONTAINERS=true to enable integration tests
if System.get_env("ENABLE_TESTCONTAINERS") == "true" and Code.ensure_loaded?(Testcontainers) do
  # Ensure hackney application is started (required by Tesla/testcontainers)
  Application.ensure_all_started(:hackney)

  try do
    {:ok, _} = Testcontainers.start_link()
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end
end

ExUnit.configure(exclude: [:integration])
ExUnit.start()
