#!/usr/bin/env -S mix run --no-mix-exs
Code.require_file("common.exs")
Code.ensure_loaded(Map)
Code.ensure_loaded(Enum)
Code.ensure_loaded(:prim_file)
Code.ensure_loaded(:erlang)
Code.ensure_loaded(:binary)

# require ReadMeasurements

defmodule ReadMeasurements.App do
  def main do
    filename = "./measurements.txt"

    worker_count = ReadMeasurements.worker_count()

    # IO.puts "Using #{worker_count} workers"

    parent = self()

    workers = Enum.map(1..worker_count, fn _ ->
      spawn_link(fn ->
        worker_main(parent, "")
      end)
    end)

    {:ok, file} = :prim_file.open(filename, [:raw, :binary, :read])
    result =
      try do
        read_file(file, workers)
      after
        :prim_file.close(file)
      end

    result
    |> Enum.map(fn {ws, {count, total, min, max}} ->
      # we were working in fixed point to we need to turn these back into floats for the output
      {ws, {min / 10.0, (total / count) / 10.0, max / 10.0}}
    end)
    |> ReadMeasurements.output()
  end

  def worker_main(parent, <<>>) do
    send(parent, {:checkin, self()})
    receive do
      :eos ->
        # we don't have to care about the type unlike the erlang version
        # so we can just take the process dictionary as is
        send(parent, {:result, :erlang.get()})
        :ok

      {:chunk, bin} ->
        worker_main(parent, bin)
    end
  end

  def worker_main(parent, rest) do
    worker_main(
      parent,
      process_line(rest)
    )
  end

  def read_file(file, workers) do
    :ok = do_read_file(file)

    Enum.map(workers, fn _worker ->
      receive do
        {:checkin, _} ->
          :ok
      end
    end)

    workers
    |> Enum.reduce(%{}, fn worker, result ->
      send(worker, :eos)
      receive do
        {:result, result2} ->
          Enum.reduce(result2, result, fn {ws, {rcount, rtotal, rmin, rmax} = row}, result ->
            case Map.fetch(result, ws) do
              :error ->
                Map.put(result, ws, row)

              {:ok, {rcount2, rtotal2, rmin2, rmax2}} ->
                Map.put(result, ws, {rcount + rcount2, rtotal + rtotal2, min(rmin, rmin2), max(rmax, rmax2)})
            end
          end)
      end
    end)
    |> Enum.sort_by(fn {key, _} ->
      key
    end)
  end

  def do_read_file(file) do
    # dropping down to barebones file to skip some of the overhead
    case :prim_file.read(file, ReadMeasurements.blob_size()) do
      :eof ->
        :ok

      {:ok, bin} ->
        bin =
          case :prim_file.read_line(file) do
            :eof ->
              bin

            {:ok, line} ->
              <<bin::binary, line::binary>>
          end

        receive do
          {:checkin, worker} ->
            send(worker, {:chunk, bin})
        end
        do_read_file(file)
    end
  end

  defp process_line(rest) do
    parse_weather_station(rest, rest, 0)
  end

  defp parse_weather_station(bin, <<";",_rest::binary>>, count) do
    <<ws::binary-size(count), ";", rest::binary>> = bin
    parse_temp(rest, ws)
  end

  defp parse_weather_station(bin, <<_c,rest::binary>>, count) do
    parse_weather_station(bin, rest, count + 1)
  end

  defmacrop char_to_num(c) do
    quote do
      (unquote(c) - ?0)
    end
  end

  defp parse_temp(<<?-, d2, d1, ?., d01, "\n", rest::binary>>, ws) do
    commit_entry(ws, -(char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01)))
    rest
  end

  defp parse_temp(<<?-, d1, ?., d01, "\n", rest::binary>>, ws) do
    commit_entry(ws, -(char_to_num(d1) * 10 + char_to_num(d01)))
    rest
  end

  defp parse_temp(<<d2, d1, ?., d01, "\n", rest::binary>>, ws) do
    commit_entry(ws, char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01))
    rest
  end

  defp parse_temp(<<d1, ?., d01, "\n", rest::binary>>, ws) do
    commit_entry(ws, char_to_num(d1) * 10 + char_to_num(d01))
    rest
  end

  defp commit_entry(ws, temp) do
    # write it to the process dictionary
    case :erlang.get(ws) do
      :undefined ->
        :erlang.put(ws, {1, temp, temp, temp})

      {count, total, mn, mx} ->
        :erlang.put(ws, {count + 1, total + temp, min(mn, temp), max(mx, temp)})
    end
  end
end

# ReadMeasurements.with_profiling do
  ReadMeasurements.App.main()
# end
