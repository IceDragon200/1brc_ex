# 1BRC for Elixir

The files only operate on a small dataset of 50 million lines, but it should scale linearly to 1 billion lines.

Each implementation is a permutation that seeks to experiment or optimize with a particular function or pattern.

Files in the `src/` directory are those that are contenders for the best performance.

Files in the `disq/` were implementations that didn't quite match up to the needed performance of those found in `src/`.

## Setup

Either copy or symlink your measurements.txt into the root directory of this repo, then run a file from the src/ or disq/ directories.

```bash
ln -sf /data/1brc/measurements.txt ./measurements.txt
```
