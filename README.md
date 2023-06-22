Zhang Y, Li Y, Guo F, et al. ElasticBF: Fine-grained and Elastic Bloom Filter Towards Efficient Read for LSM-tree-based KV Stores[C]//HotStorage. 2018.

[![ci](https://github.com/google/leveldb/actions/workflows/build.yml/badge.svg)](https://github.com/google/leveldb/actions/workflows/build.yml)

# Features
![The architecture of ElasticBF](https://github.com/WangTingZheng/Paperdb/assets/32613835/cb3278c6-9782-48b1-bda4-2051713a6a97))

* Fine-grained Bloom Filter Allocation
* Hotness Identification
* Bloom Filter Management in Memory

# Getting the Source

```bash
git clone --recurse-submodules https://github.com/WangTingZheng/Paperdb.git
git checkout elasticbf-dev
```

# Building

This project supports [CMake](https://cmake.org/) out of the box.

### Build for POSIX

Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_SAN=ON .. && cmake --build .
```
