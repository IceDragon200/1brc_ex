#!/usr/bin/env -S mix run --no-mix-exs
# This is a test file to see how long it takes to read the measurements file.
# Note that it's just reading 1mb chunks of the file and throwing it out
Code.ensure_loaded(:eprof)
Code.ensure_loaded(:prim_file)

defmodule ReadMeasurements.Test do
  def read_file(filename, parent) do
    {:ok, fd} = :prim_file.open(filename, [:binary, :read])

    try do
      do_read_file(fd, parent)
    after
      :prim_file.close(fd)
    end
  end

  def do_read_file(fd, parent) do
    # dropping down to barebones file to skip some of the overhead
    case :prim_file.read(fd, 0x8_000_000) do
      :eof ->
        send(parent, :eos)

      bin ->
        send(parent, {:chunk, bin})
        do_read_file(fd, parent)
    end
  end

  def main do
    receive do
      {:chunk, _bin} ->
        main()

      :eos ->
        :ok
    end
  end
end

try do
  #filename = "./measurements.txt"
  filename = "/data/1brc/measurements.txt"

  parent = spawn_link(&ReadMeasurements.Test.main/0)
  ReadMeasurements.Test.read_file(filename, parent)
after
  :ok
end
