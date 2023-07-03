#!/bin/bash

# false: use default db path
# true:  use user's db path
classify_path=false
# get db path from user
if [[ $# -eq 0 ]]; then
  echo "please entry model"
  exit 1
fi

if [[ $# -ge 1 ]]; then
  if [[ $1 == --model=* ]]; then
    model=$(echo $1 | cut -d= -f2)
    if [[ $model != "read" ]] && [[ $model != "write" ]] && [[ $model != "clean" ]]; then
        echo "wrong model, must be read or write or clean"
        exit 1
    fi
  else
    echo "Wrong model format, please append your model after --model= like --model=read!"
    exit 1
  fi
fi

if [[ $# -ge 2 ]]; then
  if [[ $2 == --db_path=* ]]; then
      db_path=$(echo $2 | cut -d= -f2)
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

benchmark="./db_bench --num=200000000 --print_process=0 --value_size=1024 --reads=10000000 --bloom_bits=4"

# add model prefix
if [[ $model == "read" ]]; then
  benchmark+=" --benchmarks=readrandom"
  benchmark+=" --use_existing_db=1"
elif [[ $model == "write" ]]; then
  benchmark+=" --benchmarks=fillrandom"
elif [[ $model == "clean" ]]; then
  benchmark+=" --benchmarks=crc32c"
else
  echo "wrong model, must be read or write or clean"
  exit 1
fi

# pass in db path
if [[ $classify_path == true ]]; then
  benchmark+=" --db=$db_path"
fi

$benchmark "--multi_queue_open=0"
$benchmark "--multi_queue_open=1"

# clean the cmake and make file
cd ../
rm -rf build