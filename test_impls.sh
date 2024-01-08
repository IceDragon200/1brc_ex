#!/usr/bin/env bash
rm -rf out/src/*.log
mkdir -p out/src
for f in ./src/*.exs ; do
	echo $f
	time $($f > out/$f.log)
done
