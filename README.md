Zhang Y, Li Y, Guo F, et al. ElasticBF: Fine-grained and Elastic Bloom Filter Towards Efficient Read for LSM-tree-based KV Stores[C]//HotStorage. 2018.

[![ci](https://github.com/google/leveldb/actions/workflows/build.yml/badge.svg)](https://github.com/google/leveldb/actions/workflows/build.yml)

# Features
![The architecture of ElasticBF](https://github.com/WangTingZheng/Paperdb/assets/32613835/cb3278c6-9782-48b1-bda4-2051713a6a97)

* Fine-grained Bloom Filter Allocation
* Hotness Identification
* Bloom Filter Management in Memory

# Getting the Source

```bash
git clone --recurse-submodules https://github.com/WangTingZheng/Paperdb.git
git checkout elasticbf-dev
```

# Building

This project supports [CMake](https://cmake.org/) out of the box. Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```

# External Parameters for cmake

## Google sanitizers

Sanitizers only support of Debug mod, and you must turn it on by yourself:
```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SAN=ON .. && cmake --build .
```

## Bloom filter adjustment logging

Turn on logging by passing parameter in cmake command:

```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
```
or in release mod:

```bash
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
```

You can check out adjustment information in ``/LOG`` file in your db dictionary:
```bash
cd /your/db/dictionary
cat LOG | grep "Adjustment:"
```
Note: you must delete cmake cache file before switch ```USE_ADJUSTMENT_LOGGING``` parameters
# Main changed files

* util/bloom.cc: Generate and read multi filter units
* table/filterblock.cc: Manage multi filter units in disk, update the hotness of the SSTable. 
* table/table.cc: Check if the key is existed by access multi filter units
* table/table_builder.cc: Construct filter block
* util/multi_queue.cc: Manage filter units in memory to reduce adjust overhead

# Benchmark
todo

# ToDo
