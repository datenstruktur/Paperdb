#!/bin/bash

if [[ $1 == --db_path=* ]]; then
  db_path=$(echo $1 | cut -d= -f2)
  if [[ -d $db_path ]]; then
    echo "your db path is in $db_path"
  else
    echo "There is no this path $db_path in your os!"
    exit 1
  fi
else
  echo "Wrong db path format, please append your db path after --db_path=/your/db/path!"
  exit 1
fi

rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
./db_bench --multi_queue_open=0 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom --db=$db_path
./db_bench --multi_queue_open=1 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom --db=$db_path
cd ../
rm -rf build