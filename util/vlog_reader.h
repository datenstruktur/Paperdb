//
// Created by WangTingZheng on 2023/6/27.
//

#ifndef STORAGE_LEVELDB_UTIL_VLOG_READER_H_
#define STORAGE_LEVELDB_UTIL_VLOG_READER_H_

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "arena.h"

namespace leveldb {

class VlogReader {
public:
  explicit VlogReader(RandomAccessFile* file);

  Status EncodingValue(Slice handle, Slice *value, Arena *arena);

  static bool GetEntrySize(Slice handle, uint64_t * entry_size);

  bool ReadRecond(Slice handle, Slice* value, char* buf, uint64_t value_size);

 private:
  RandomAccessFile* file_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_VLOG_READER_H_
