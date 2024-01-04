#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("output.exs")

try do
  filename = "./measurements.txt"
  reduce = fn reduce, file, result ->
    case IO.read(file, :line) do
      :eof ->
        result

      bin when is_binary(bin) ->
        [ws, temp] = :binary.split(bin, ";")
        temp =
          case Float.parse(temp) do
            {temp, "\n"} ->
              temp

            {temp, ""} ->
              temp
          end
        reduce.(
          reduce,
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

  {:ok, result} =
    File.open(filename, [:read, :utf8], fn file ->
      reduce.(reduce, file, %{})
    end)

  result = Enum.sort_by(result, fn {ws, _} ->
    ws
  end)

  Output.output(result)
after
  :ok
end
