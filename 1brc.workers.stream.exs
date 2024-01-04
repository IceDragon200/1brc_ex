#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("output.exs")

try do
  filename = "./measurements.txt"

  worker_count = :erlang.system_info(:logical_processors) * 4
  chunk_size = 100_000

  IO.puts "Using #{worker_count} workers"

  main = fn
    main, [bin | rest], result ->
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
        rest,
        case Map.fetch(result, ws) do
          :error ->
            Map.put(result, ws, {temp, temp, temp})

          {:ok, {mn, mean, mx}} ->
            Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
        end
      )

    main, [], result ->
      receive do
        {:eos, parent} ->
          send(parent, result)
          :ok

        bins ->
          main.(main, bins, result)
      end
  end

  workers = Enum.map(1..worker_count, fn _ ->
    spawn_link(fn ->
      main.(main, [], %{})
    end)
  end)

  File.stream!(filename, :line)
  |> Stream.chunk_every(chunk_size)
  |> Enum.reduce({workers, []}, fn
    bins, {[worker | workers], workers2} ->
      send(worker, bins)
      {workers, [worker | workers2]}

    bins, {[] = workers, [worker | workers2]} ->
      send(worker, bins)
      {workers2, [worker | workers]}
  end)

  result =
    workers
    |> Enum.reduce(%{}, fn worker, result ->
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
    |> Enum.sort_by(fn {key, _} ->
      key
    end)

  Output.output(result)
after
  :ok
end
