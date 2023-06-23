# ElasticBF
[![ci](https://github.com/google/leveldb/actions/workflows/build.yml/badge.svg)](https://github.com/google/leveldb/actions/workflows/build.yml)

**ElasticBF[1]** moves cold SSTable's bloom filter to hot SSTable's bloom filter to reduce extra disk io overhead without increasing memory overhead.

# Features
![The architecture of ElasticBF](https://github.com/WangTingZheng/Paperdb/assets/32613835/cb3278c6-9782-48b1-bda4-2051713a6a97)

* Fine-grained Bloom Filter Allocation
* Hotness Identification
* Bloom Filter Management in Memory 

> More details see in [doc/elasticbf.md](./doc/elasticbf.md), Chinese version see in [feishu](https://o444bvn7jh.feishu.cn/docx/XlBldwKc2oOTGMxPpLlckKLunvd), If you are interested in gaining a deeper understanding of the paper, please do not hesitate to contact me. I have added extensive annotations to the original paper, and I have recorded 8 hours of lecture videos to elaborate on my interpretation of the paper.

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

# External parameters for cmake

## Google sanitizers

Sanitizers only support of Debug mod, and you must turn it on by yourself:
```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SAN=ON .. && cmake --build .
```

Why google sanitizers? Google sanitizers is faste more than 10x with vaigrind[2]. We close it in order to speed up ci.

## Bloom filter adjustment logging

Turn on logging by passing parameter in cmake command:

```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
```
or in release mod:

```bash
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
```

You can check out adjustment information in ``/LOG`` file in your db dictionary, in ``/tmp`` in default setting:
```bash
cd /your/db/dictionary
cat LOG | grep "Adjustment:"
```
**Note1**: You must delete cmake cache file(such as cmake-build-debug) before switching ```USE_ADJUSTMENT_LOGGING``` parameters' value

**Note2**: Adjustment only be active when db has a lot of KV(100GB in paper), and a lot of get requests(10milion in paper).

# Unit test and benchmark

## unit test:
```bash
cd build

./leveldb_tests
./env_posix_test
./c_test
```
## benchmark
```bash
./db_bench
```

## benchmark parameters:

close multi_queue, use default way to get filter 
```bash
./db_bench --multi_queue_open=0
```
close info printing in FinishedSingleOp
```bash
./db_bench --print_process=0
```

# Main changed files

* **util/bloom.cc**: Generate and read multi filter units' bitmap for a scope of keys
* **table/filterblock.cc**: Manage multi filter units in disk, update the hotness of the SSTable. 
* **table/table.cc**: Check if the key is existed using filterblock
* **table/table_builder.cc**: Construct filter block
* **util/multi_queue.cc**: Manage filter units in memory to reduce adjust overhead

# Performance

## Setup

### Hardware

Todo

### parameters
* 100GB kv in database
* 10 million point lookup
* 4 bits per key in one filter unit
* 6 filter units for one sstable
* 1KByte KV

## Write performance

Todo
## Read performance
Todo
# ToDo
- Using multi thread to speed up filter units loading in multi queue
- Using shared hash in this paper[3] to reduce multi bloom filter look up overhead
- Hotness inheritance after compaction in ATC version of paper[4]
- Using perf tool to find code can be optimized


# Reference
[1] Zhang Y, Li Y, Guo F, et al. ElasticBF: Fine-grained and Elastic Bloom Filter Towards Efficient Read for LSM-tree-based KV Stores[C]//HotStorage. 2018.

[2] 王留帅、徐明杰-Sanitizer 在字节跳动 C C++ 业务中的实践. https://www.bilibili.com/video/BV1YT411Q7BU

[3] Zhu Z, Mun J H, Raman A, et al. Reducing bloom filter cpu overhead in lsm-trees on modern storage devices[C]//Proceedings of the 17th International Workshop on Data Management on New Hardware (DaMoN 2021). 2021: 1-10.

[4] Li Y, Tian C, Guo F, et al. ElasticBF: Elastic Bloom Filter with Hotness Awareness for Boosting Read Performance in Large Key-Value Stores[C]//USENIX Annual Technical Conference. 2019: 739-752.