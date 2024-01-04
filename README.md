# 1BRC for Elixir

The files only operate on a small dataset of 50 million lines, but it should scale linearly to 1 billion lines.

Four implementations were created to test different ways of doing effectively the same task.

`*.reduce.exs` files manually call `IO.read(device, :line)` to pull a line from the file stream, this is considerably slower than just using `File.stream(filename, :line)`
`*.stream.exs` files use Elixir's Streams to extract lines from the file, these are considerably faster than manually reduce using IO.read

## Testing

Simply symlink or copy your generated measurements.txt into the directory with the script and run anyone of them to your liking.
