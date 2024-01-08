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
    result = do_read_file(file, <<>>, nil, nil, 0, :ets.new(:result, [:private, :set]))

    result
    |> :ets.tab2list()
    |> Enum.sort_by(fn {key, _, _, _} ->
      key
    end)
    |> Enum.map(fn {key, a, b, c} ->
      {key, {a, b, c}}
    end)
  end

  def do_read_file(file, rest, ws, temp, c, result) do
    # dropping down to barebones file to skip some of the overhead
    case :file.read(file, ReadMeasurements.blob_size()) do
      :eof ->
        case rest do
          "" ->
            result
        end

      {:ok, bin} ->
        {elapsed, {rest, ws, temp, c, result}} = :timer.tc(fn ->
          do_split_all(rest <> bin, ws, temp, c, result)
        end)
        # IO.inspect({elapsed / 1_000_000, c})
        do_read_file(file, rest, ws, temp, c, result)
    end
  end

  defp do_split_all(rest, nil, nil, c, result) do
    case :binary.split(rest, ";") do
      [ws, rest] ->
        do_split_all(rest, ws, nil, c + 1, result)

      [rest] ->
        {rest, nil, nil, c, result}
    end
  end

  defp do_split_all(rest, ws, nil, c, result) do
    case :binary.split(rest, "\n") do
      [temp, rest] ->
        # Float.parse is really slow, so we need to drop to erlang
        temp = :erlang.binary_to_float(temp)

        case :ets.lookup(result, ws) do
          [] ->
            :ets.insert(result, {ws, temp, temp, temp})

          [{ws, mn, mean, mx}] ->
            :ets.insert(
              result,
              {ws, min(mn, temp), (mean+temp)/2, max(mx, temp)}
            )
        end

        do_split_all(rest, nil, nil, c, result)

      [rest] ->
        {rest, ws, nil, c, result}
    end
  end
end

ReadMeasurements.App.main()
