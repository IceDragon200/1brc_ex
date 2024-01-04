defmodule Output do
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
