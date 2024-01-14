#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")
Code.ensure_loaded(Map)
Code.ensure_loaded(Enum)
Code.ensure_loaded(:prim_file)
Code.ensure_loaded(:erlang)
Code.ensure_loaded(:binary)

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"

    worker_count = ReadMeasurements.worker_count()
    chunk_size = ReadMeasurements.chunk_size()

    # IO.puts "Using #{worker_count} workers"

    parent = self()

    workers = Enum.map(1..worker_count, fn _ ->
      spawn_link(fn ->
        worker_main(parent, [], %{})
      end)
    end)

    :ok =
      File.stream!(filename, :line)
      |> Stream.chunk_every(chunk_size)
      |> Stream.each(fn
        bins ->
          receive do
            {:checkin, worker} ->
              # IO.write "I"
              send(worker, bins)
              workers
          end
      end)
      |> Stream.run()

    Enum.map(workers, fn _worker ->
      receive do
        {:checkin, _} ->
          :ok
      end
    end)

    result =
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

    ReadMeasurements.output(result)
  end

  def worker_main(parent, [bin | rest], result) do
    [ws, temp] = :binary.split(bin, ";")
    [temp, ""] = :binary.split(temp, "\n")
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
    # IO.write "O"
    send(parent, {:checkin, self()})
    receive do
      :eos ->
        send(parent, {:result, result})
        :ok

      bins ->
        worker_main(parent, bins, result)
    end
  end
end

ReadMeasurements.App.main()
