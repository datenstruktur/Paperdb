//
// Created by 14037 on 2023/7/29.
//

#ifndef LEVELDB_READ_BUFFER_H
#define LEVELDB_READ_BUFFER_H

#include <cstdlib>
#include <malloc.h>
#include <unistd.h>
#include <cstdint>

namespace leveldb {
namespace {
size_t kAlignPageSize = 0;

size_t GetPageSize() {
  if (kAlignPageSize > 0) return kAlignPageSize;
#if __linux__ || defined(_SC_PAGESIZE)
  long v = sysconf(_SC_PAGESIZE);
  if (v >= 1024) {
    kAlignPageSize = static_cast<size_t>(v);
  } else {
    // Default assume 4KB
    kAlignPageSize = 4U * 1024U;
  }
#elif _WIN32
#include <sysinfoapi.h>
  SYSTEM_INFO sinfo;
  GetSystemInfo(&sinfo);

  page_size = sinfo.dwPageSize;
#else
  // Default assume 4KB
  page_size = 4U * 1024U;
#endif

  return kAlignPageSize;
}
}

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

inline static bool IsAligned(uint64_t val){
  return (val & (GetPageSize() - 1)) == 0;
}

inline static bool IsAligned(const char* ptr){
  return (reinterpret_cast<uintptr_t>(ptr) & (GetPageSize() - 1)) == 0;
}

}  // namespace leveldb

#endif  // LEVELDB_READ_BUFFER_H
