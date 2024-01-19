Code.ensure_loaded(Enum)

defmodule ReadMeasurements do
  defmacro with_profiling(do: expression) do
    quote do
      :eprof.start()
      :eprof.start_profiling(:erlang.processes())
      unquote(expression)
      :eprof.stop_profiling()
      :eprof.analyze()
      :eprof.stop()
    end
  end

  def worker_count do
    # Once upon a time, I cranked this up 4x whatever the logical processors were
    # the bad news: this is a waste of resources as the processes will now fight over the CPUs
    # Instead it should probably be 1:1
    :erlang.system_info(:logical_processors)
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
