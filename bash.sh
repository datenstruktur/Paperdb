#!/bin/bash

# false: use default db path
# true:  use user's db path
classify_path=false

# get db path from user
if [[ $# -ne 0 ]]; then
  if [[ $1 == --db_path=* ]]; then
    db_path=$(echo $1 | cut -d= -f2)
    if [[ -d $db_path ]]; then
      echo "Your db path is in $db_path"
      classify_path=true
    else
      echo "There is no this path $db_path in your os!"
      exit 1
    fi
  else
    echo "Wrong db path format, please append your db path after --db_path= like --db_path=/your/db/path!"
    exit 1
  fi
fi

# clean and compile project
rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .

# pass in db path
if [[ $classify_path == true ]]; then
  ./db_bench --multi_queue_open=0 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom --db=$db_path
  ./db_bench --multi_queue_open=1 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom --db=$db_path
else
  ./db_bench --multi_queue_open=0 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom
  ./db_bench --multi_queue_open=1 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom
fi

# clean the cmake and cmake file
cd ../
rm -rf build