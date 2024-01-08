#!/usr/bin/env -S mix run --no-mix-exs
# This is a test file to see how long it takes to read the measurements file.
# Note that it's just reading 1mb chunks of the file and throwing it out

defmodule ReadMeasurements do
  def read_file(filename, parent) do
    {:ok, file} = File.open(filename, [:binary, :read])

    try do
      do_read_file(file, parent)
    after
      File.close(file)
    end
  end

  def do_read_file(file, parent) do
    # dropping down to barebones file to skip some of the overhead
    case :file.read(file, 0x8_000_000) do
      :eof ->
        send(parent, :eos)

      bin ->
        send(parent, {:chunk, bin})
        do_read_file(file, parent)
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
  filename = "./measurements.txt"

  parent = spawn_link(&ReadMeasurements.main/0)

  ReadMeasurements.read_file(filename, parent)
after
  :ok
end
