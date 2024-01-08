#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")

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

    {:ok, file} = File.open(filename, [:binary, :read])
    result =
      try do
        read_file(file, workers)
      after
        File.close(file)
      end

    ReadMeasurements.output(result)
  end

  def worker_main(parent, [bin | rest], result) do
    [ws, temp] = :binary.split(bin, ";")
    temp = :erlang.binary_to_float(temp)

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
    case :file.read(file, ReadMeasurements.blob_size()) do
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
end

ReadMeasurements.App.main()
