#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"
    worker_count = ReadMeasurements.worker_count()

    # IO.puts "Using #{worker_count} workers"

    workers = Enum.map(1..worker_count, fn _ ->
      spawn_link(fn ->
        worker_main(%{})
      end)
    end)

    reduce = fn
      reduce, file, result, [] = workers, workers2 ->
        reduce.(reduce, file, result, workers2, workers)

      reduce, file, result, [worker | workers], workers2 ->
        case IO.read(file, :line) do
          {:error, _} = err ->
            throw err

          :eof ->
            workers = [worker | workers] ++ workers2
            Enum.reduce(workers, result, fn worker, result ->
              send(worker, {:eos, self()})
              receive do
                result2 ->
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

          bin ->
            send(worker, bin)
            reduce.(reduce, file, result, workers, [worker | workers2])
        end
    end

    {:ok, result} =
      File.open(filename, [:read, :utf8], fn file ->
        reduce.(reduce, file, %{}, workers, [])
      end)

    result = Enum.sort_by(result, fn {ws, _} ->
      ws
    end)

    ReadMeasurements.output(result)
  end

  def worker_main(result) do
    receive do
      {:eos, parent} ->
        send(parent, result)
        :ok

      bin ->
        [ws, temp] = :binary.split(bin, ";")
        [temp, ""] = :binary.split(temp, "\n")
        temp = :erlang.binary_to_float(temp)

        worker_main(
          case Map.fetch(result, ws) do
            :error ->
              Map.put(result, ws, {temp, temp, temp})

            {:ok, {mn, mean, mx}} ->
              Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
          end
        )
    end
  end
end

ReadMeasurements.App.main()
