# ElasticBF
[![ci](https://github.com/google/leveldb/actions/workflows/build.yml/badge.svg)](https://github.com/google/leveldb/actions/workflows/build.yml)

**ElasticBF[1]** moves cold SSTable's bloom filter to hot SSTable's bloom filter to reduce extra disk io overhead without increasing memory overhead. Short introduction by paper's author can be saw in this [video](https://www.youtube.com/watch?v=8UBx3GCep3A).

# Features
![The architecture of ElasticBF](https://github.com/WangTingZheng/Paperdb/assets/32613835/cb3278c6-9782-48b1-bda4-2051713a6a97)

* Fine-grained Bloom Filter Allocation
* Hotness Identification
* Bloom Filter Management in Memory 

> More details see in [doc/elasticbf.md](./doc/elasticbf.md), Chinese version see in [feishu](https://o444bvn7jh.feishu.cn/docx/XlBldwKc2oOTGMxPpLlckKLunvd), If you are interested in gaining a deeper understanding of the paper, please do not hesitate to contact me. I have added extensive annotations to the original paper, and I have recorded 8 hours of lecture videos to elaborate on my interpretation of the paper.

# Getting the Source

```shell
git clone --recurse-submodules https://github.com/WangTingZheng/Paperdb.git
cd Paperdb
git checkout elasticbf-dev
```

# Building

This project supports [CMake](https://cmake.org/) out of the box. Quick start:

```shell
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```

# External parameters for cmake

## Google sanitizers

Sanitizers only support of Debug mod, and you must turn it on by yourself:
```shell
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SAN=ON .. && cmake --build .
```

Why google sanitizers? Google sanitizers is faster more than 10x with vaigrind[2]. We close it in order to speed up ci.

## Bloom filter adjustment logging

Turn on logging by passing parameter in cmake command:

```shell
cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
```
or in release mod:

```shell
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
```

You can check out adjustment information in ``/LOG`` file in your db dictionary, in ``/tmp`` in default setting:
```shell
cd /your/db/dictionary
cat LOG | grep "Adjustment:"
```
**Note1**: You must delete cmake cache file(such as cmake-build-debug) before switching ```USE_ADJUSTMENT_LOGGING``` parameters' value

**Note2**: Adjustment only be active when db has a lot of KV(100GB in paper), and a lot of get requests(10milion in paper).

# Unit test and benchmark

## unit test:
```shell
cd build
./leveldb_tests
./env_posix_test
./c_test
```
## benchmark
```shell
cd build
./db_bench
```

## benchmark parameters:

close multi_queue, use default way to get filter 
```shell
./db_bench --multi_queue_open=0
```
close info printing in FinishedSingleOp
```shell
./db_bench --print_process=0
```

**Note**: We disable FinishedSingleOp printf in CI to track error easier.
# Main changed files

* **util/bloom.cc**: Generate and read multi filter units' bitmap for a scope of keys **(97% lines unit test coverage)**
* **table/filterblock.cc**: Manage multi filter units in disk, update the hotness of the SSTable. **(94% lines unit test coverage)** 
* **table/table.cc**: Check if the key is existed using filterblock **(97% lines unit test coverage)**
* **table/table_builder.cc**: Construct filter block **(89% lines unit test coverage)**
* **util/multi_queue.cc**: Manage filter units in memory to reduce adjust overhead **(98% lines unit test coverage)**
# Performance

when run benchmark through bash.sh, you can pass in your own dictionary 
```shell
chmod +x bash.sh
./bash.sh --db_path=/your/db/path
```

If you want to use default dictionary, just run:
```shell
./bash.sh
```

## Setup

### Hardware

* CPU:        32 * 13th Gen Intel(R) Core(TM) i9-13900K
* CPUCache:   36864 KB
* SSD:        Samsung SSD 870(4TB)

### parameters
* 100GB kv in database
* 10 million point lookup
* 4 bits per key in one filter unit
* 6 filter units for one SSTable
* Load 2 filters at the beginning
* 1KB KV
* On release model
* Snappy compression is not enabled

## Write performance

Todo
## Read performance
Todo
# ToDo
- Using multi thread to speed up filter units loading in multi queue, see [about implement multi threads](https://github.com/WangTingZheng/Paperdb/discussions/14).
- Using shared hash in this paper[3] to reduce multi bloom filter look up overhead.
- Hotness inheritance after compaction in ATC version of paper[4], see [about implement hotness inheritance](https://github.com/WangTingZheng/Paperdb/discussions/13) in discussions.
- Using perf tool to find code can be optimized.

# Next Paper

Next paper to implement maybe AC-Key[5] or HotRing[6] or **WiscKey[7]**, see [some idea about implement Wisckey](https://github.com/WangTingZheng/Paperdb/discussions/12).

# Reference
[1] Zhang Y, Li Y, Guo F, et al. ElasticBF: Fine-grained and Elastic Bloom Filter Towards Efficient Read for LSM-tree-based KV Stores[C]//HotStorage. 2018.

[2] 王留帅、徐明杰-Sanitizer 在字节跳动 C C++ 业务中的实践. https://www.bilibili.com/video/BV1YT411Q7BU. 2023.

[3] Zhu Z, Mun J H, Raman A, et al. Reducing bloom filter cpu overhead in lsm-trees on modern storage devices[C]//Proceedings of the 17th International Workshop on Data Management on New Hardware (DaMoN 2021). 2021: 1-10.

[4] Li Y, Tian C, Guo F, et al. ElasticBF: Elastic Bloom Filter with Hotness Awareness for Boosting Read Performance in Large Key-Value Stores[C]//USENIX Annual Technical Conference. 2019: 739-752.

[5] Wu F, Yang M H, Zhang B, et al. AC-key: Adaptive caching for LSM-based key-value stores[C]//Proceedings of the 2020 USENIX Conference on Usenix Annual Technical Conference. 2020: 603-615.

[6] Chen J, Chen L, Wang S, et al. HotRing: A Hotspot-Aware In-Memory Key-Value Store[C]//FAST. 2020: 239-252.

[7] Lu L, Pillai T S, Gopalakrishnan H, et al. Wisckey: Separating keys from values in ssd-conscious storage[J]. ACM Transactions on Storage (TOS), 2017, 13(1): 1-28.