//
// Created by WangTingZheng on 2023/6/27.
//

#ifndef STORAGE_LEVELDB_UTIL_VLOG_READER_H_
#define STORAGE_LEVELDB_UTIL_VLOG_READER_H_

#include "leveldb/status.h"
#include "leveldb/env.h"

namespace leveldb {

class VlogReader {
public:
  explicit VlogReader(RandomAccessFile* file);

  static bool GetValueSize(Slice handle, uint64_t *value_size);

  bool ReadRecond(Slice handle, Slice* value, char* buf, uint64_t value_size);

 private:
  RandomAccessFile* file_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_VLOG_READER_H_
