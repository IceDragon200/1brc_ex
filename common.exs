defmodule ReadMeasurements do
  def worker_count do
    :erlang.system_info(:logical_processors) * 4
  end

  def chunk_size do
    1_000_000
  end

  def blob_size do
    0x1_000_000
  end

  def output(result) do
    [
      "{",
      result
      |> Enum.map(fn {ws, {rmin, rmean, rmax}} ->
        [
          ws,
          "=",
          :erlang.float_to_binary(rmin, decimals: 1), "/",
          :erlang.float_to_binary(rmean, decimals: 1), "/",
          :erlang.float_to_binary(rmax, decimals: 1),
        ]
      end)
      |> Enum.intersperse(", "),
      "}"
    ]
    |> IO.puts()
  end
end