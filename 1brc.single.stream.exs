#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("output.exs")

try do
  filename = "./measurements.txt"
  chunk_size = 100_000

  result =
    File.stream!(filename, [:line])
    |> Stream.map(fn bin ->
      [ws, temp] = :binary.split(bin, ";")
      temp =
        case Float.parse(temp) do
          {temp, "\n"} ->
            temp

          {temp, ""} ->
            temp
        end

      {ws, temp}
    end)
    |> Stream.chunk_every(chunk_size)
    |> Enum.reduce(result, fn {ws, temp}, result ->
      case Map.fetch(result, ws) do
        :error ->
          Map.put(result, ws, {temp, temp, temp})

        {:ok, {mn, mean, mx}} ->
          Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
      end
    end)
    |> Enum.sort_by(fn {ws, _} ->
      ws
    end)

  Output.output(result)
after
  :ok
end
