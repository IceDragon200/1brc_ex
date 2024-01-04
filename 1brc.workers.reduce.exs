#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("output.exs")

try do
  filename = "./measurements.txt"
  worker_count = :erlang.system_info(:logical_processors) * 4

  IO.puts "Using #{worker_count} workers"
  main = fn main, result ->
    receive do
      {:eos, parent} ->
        send(parent, result)
        :ok

      bin ->
        [ws, temp] = :binary.split(bin, ";")
        temp =
          case Float.parse(temp) do
            {temp, "\n"} ->
              temp

            {temp, ""} ->
              temp
          end
        main.(
          main,
          case Map.fetch(result, ws) do
            :error ->
              Map.put(result, ws, {temp, temp, temp})

            {:ok, {mn, mean, mx}} ->
              Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
          end
        )
    end
  end

  workers = Enum.map(1..worker_count, fn _ ->
    spawn_link(fn ->
      main.(main, %{})
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

  Output.output(result)
after
  :ok
end
