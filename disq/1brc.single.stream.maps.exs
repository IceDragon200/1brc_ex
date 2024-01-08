#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"

    result =
      File.stream!(filename, [:line])
      |> Stream.map(fn bin ->
        [ws, temp] = :binary.split(bin, ";")
        [temp, ""] = :binary.split(temp, "\n")
        temp = :erlang.binary_to_float(temp)
        {ws, temp}
      end)
      |> Enum.reduce(%{}, fn {ws, temp}, result ->
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

    ReadMeasurements.output(result)
  end
end

ReadMeasurements.App.main()
