#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")
Code.ensure_loaded(Map)
Code.ensure_loaded(Enum)
Code.ensure_loaded(:prim_file)
Code.ensure_loaded(:erlang)
Code.ensure_loaded(:binary)

# require ReadMeasurements

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"

    worker_count = ReadMeasurements.worker_count()

    # IO.puts "Using #{worker_count} workers"

    parent = self()

    workers = Enum.map(1..worker_count, fn _ ->
      spawn_link(fn ->
        worker_main(parent, [], %{})
      end)
    end)

    {:ok, file} = :prim_file.open(filename, [:binary, :read])
    result =
      try do
        read_file(file, workers)
      after
        :prim_file.close(file)
      end

    result
    |> Enum.map(fn {ws, {min, mean, max}} ->
      # we were working in fixed point to we need to turn these back into floats for the output
      {ws, {min/10.0, mean/10.0, max/10.0}}
    end)
    |> ReadMeasurements.output()
  end

  def worker_main(parent, [bin | rest], result) do
    [ws, temp] = :binary.split(bin, ";")
    temp = binary_to_fixed_point(temp)

    worker_main(
      parent,
      rest,
      case Map.fetch(result, ws) do
        :error ->
          Map.put(result, ws, {temp, temp, temp})

        {:ok, {mn, mean, mx}} ->
          Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
      end
    )
  end

  def worker_main(parent, [], result) do
    send(parent, {:checkin, self()})
    receive do
      :eos ->
        send(parent, {:result, result})
        :ok

      bins ->
        worker_main(parent, bins, result)
    end
  end

  def read_file(file, workers) do
    do_read_file(file, workers, <<>>, 0, [])

    Enum.map(workers, fn _worker ->
      receive do
        {:checkin, _} ->
          :ok
      end
    end)

    workers
    |> Enum.reduce(%{}, fn worker, result ->
      send(worker, :eos)
      receive do
        {:result, result2} ->
          Enum.reduce(result2, result, fn {ws, {rmin, rmean, rmax} = row}, result ->
            case Map.fetch(result, ws) do
              :error ->
                Map.put(result, ws, row)

              {:ok, {rmin2, rmean2, rmax2}} ->
                Map.put(result, ws, {min(rmin, rmin2), (rmean + rmean2) / 2, max(rmax, rmax2)})
            end
          end)
      end
    end)
    |> Enum.sort_by(fn {key, _} ->
      key
    end)
  end

  def do_read_file(file, workers, rest, c, acc) when c >= 1_000_000 do
    receive do
      {:checkin, worker} ->
        send(worker, acc)
        do_read_file(file, workers, rest, 0, [])
    end
  end

  def do_read_file(file, workers, rest, c, acc) do
    # dropping down to barebones file to skip some of the overhead
    case :prim_file.read(file, ReadMeasurements.blob_size()) do
      :eof ->
        case rest do
          "" ->
            :ok
        end

      {:ok, bin} ->
        {rest, c, acc} = do_split_all(rest <> bin, c, acc)
        do_read_file(file, workers, rest, c, acc)
    end
  end

  defp do_split_all(rest, c, acc) do
    case :binary.split(rest, "\n") do
      [line, rest] ->
        do_split_all(rest, c + 1, [line | acc])

      [rest] ->
        {rest, c, acc}
    end
  end

  defmacrop char_to_num(c) do
    quote do
      unquote(c) - ?0
    end
  end

  defp binary_to_fixed_point(<<?-, d2, d1, ?., d01>>) do
    -(char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01))
  end

  defp binary_to_fixed_point(<<?-, d1, ?., d01>>) do
    -(char_to_num(d1) * 10 + char_to_num(d01))
  end

  defp binary_to_fixed_point(<<d2, d1, ?., d01>>) do
    char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01)
  end

  defp binary_to_fixed_point(<<d1, ?., d01>>) do
    char_to_num(d1) * 10 + char_to_num(d01)
  end
end

# ReadMeasurements.with_profiling do
  ReadMeasurements.App.main()
# end
