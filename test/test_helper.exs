# Start testcontainers GenServer before ExUnit
{:ok, _} = Testcontainers.start_link()

ExUnit.configure(exclude: [:integration, :integration_v2])
ExUnit.start()
