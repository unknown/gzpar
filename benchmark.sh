#!/bin/bash

for i in {1..5}; do
  echo "Run $i:"
  time target/release/gzpar test/soc-LiveJournal1.txt
  rm -f test/soc-LiveJournal1.txt.gz
  echo "----------------"
done 
