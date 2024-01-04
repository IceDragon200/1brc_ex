#!/usr/bin/env bash
echo "Single + Stream"
time ./1brc.single.stream.exs
echo "Workers + Stream"
time ./1brc.workers.stream.exs

echo "Single + Reduce"
time ./1brc.single.reduce.exs
echo "Workers + Reduce"
time ./1brc.workers.reduce.exs
