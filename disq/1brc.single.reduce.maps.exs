#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"

    {:ok, result} =
      File.open(filename, [:read, :utf8], fn file ->
        reduce(file, %{})
      end)

    result = Enum.sort_by(result, fn {ws, _} ->
      ws
    end)

    ReadMeasurements.output(result)
  end

  def reduce(file, result) do
    case IO.read(file, :line) do
      :eof ->
        result

      bin when is_binary(bin) ->
        [ws, temp] = :binary.split(bin, ";")
        [temp, ""] = :binary.split(temp, "\n")
        temp = :erlang.binary_to_float(temp)

        reduce(
          file,
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
