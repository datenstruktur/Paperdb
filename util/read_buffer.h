//
// Created by 14037 on 2023/7/29.
//

#ifndef LEVELDB_READ_BUFFER_H
#define LEVELDB_READ_BUFFER_H

#include <cstdlib>
#include <cstdint>
#include "leveldb/env.h"

namespace leveldb {
class ReadBuffer {
 public:
  ReadBuffer();
  ~ReadBuffer();

  ReadBuffer(const ReadBuffer&) = delete;
  ReadBuffer operator=(const ReadBuffer&) = delete;

  void SetPtr(char* ptr, bool aligned);
 private:
  void FreePtr();
  char* ptr_;
  bool aligned_;
};

inline bool IsAligned(uint64_t val, size_t alignment){
  return (val & (alignment - 1)) == 0;
}

inline bool IsAligned(const char* ptr, size_t alignment){
  return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1)) == 0;
}

}  // namespace leveldb

#endif  // LEVELDB_READ_BUFFER_H
