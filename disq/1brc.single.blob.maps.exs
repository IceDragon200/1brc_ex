#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"

    {:ok, file} = File.open(filename, [:binary, :read])
    result =
      try do
        read_file(file)
      after
        File.close(file)
      end

    ReadMeasurements.output(result)
  end

  def read_file(file) do
    result = do_read_file(file, <<>>, 0, %{})

    result
    |> Enum.sort_by(fn {key, _} ->
      key
    end)
  end

  def do_read_file(file, rest, c, result) do
    # dropping down to barebones file to skip some of the overhead
    case :file.read(file, ReadMeasurements.blob_size()) do
      :eof ->
        case rest do
          "" ->
            result
        end

      {:ok, bin} ->
        {_elapsed, {rest, c, result}} = :timer.tc(fn ->
          do_split_all(rest <> bin, c, result)
        end)
        # IO.inspect({elapsed / 1_000_000, c})
        do_read_file(file, rest, c, result)
    end
  end

  defp do_split_all(rest, c, result) do
    case :binary.split(rest, "\n") do
      [line, rest] ->
        [ws, temp] = :binary.split(line, ";")
        # Float.parse is really slow, so we need to drop to erlang
        temp = :erlang.binary_to_float(temp)

        result =
          case Map.fetch(result, ws) do
            :error ->
              Map.put(result, ws, {temp, temp, temp})

            {:ok, {mn, mean, mx}} ->
              Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
          end

        do_split_all(rest, c + 1, result)

      [rest] ->
        {rest, c, result}
    end
  end
end

ReadMeasurements.App.main()
