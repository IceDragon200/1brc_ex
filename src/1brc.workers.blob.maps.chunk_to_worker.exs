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
        worker_main(parent, "", %{})
      end)
    end)

    {:ok, file} = :prim_file.open(filename, [:binary, :read])
    result =
      try do
        read_file(file, workers)
      after
        :prim_file.close(file)
      end

    result
    |> Enum.map(fn {ws, {min, mean, max}} ->
      # we were working in fixed point to we need to turn these back into floats for the output
      {ws, {min/10.0, mean/10.0, max/10.0}}
    end)
    |> ReadMeasurements.output()
  end

  def worker_main(parent, <<>>, result) do
    send(parent, {:checkin, self()})
    receive do
      :eos ->
        send(parent, {:result, result})
        :ok

      {:chunk, bin} ->
        worker_main(parent, bin, result)
    end
  end

  def worker_main(parent, rest, result) do
    [ws, rest] = :binary.split(rest, ";")
    {temp, <<"\n",rest::binary>>} = binary_split_to_fixed_point(rest)

    worker_main(
      parent,
      rest,
      case Map.fetch(result, ws) do
        :error ->
          Map.put(result, ws, {temp, temp, temp})

        {:ok, {mn, mean, mx}} ->
          Map.put(result, ws, {min(mn, temp), (mean+temp)/2, max(mx, temp)})
      end
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

  defmacrop char_to_num(c) do
    quote do
      unquote(c) - ?0
    end
  end

  defp binary_split_to_fixed_point(<<?-, d2, d1, ?., d01, rest::binary>>) do
    {-(char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01)), rest}
  end

  defp binary_split_to_fixed_point(<<?-, d1, ?., d01, rest::binary>>) do
    {-(char_to_num(d1) * 10 + char_to_num(d01)), rest}
  end

  defp binary_split_to_fixed_point(<<d2, d1, ?., d01, rest::binary>>) do
    {char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01), rest}
  end

  defp binary_split_to_fixed_point(<<d1, ?., d01, rest::binary>>) do
    {char_to_num(d1) * 10 + char_to_num(d01), rest}
  end
end

# ReadMeasurements.with_profiling do
  ReadMeasurements.App.main()
# end
