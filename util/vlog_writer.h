//
// Created by WangTingZheng on 2023/6/27.
//

#ifndef STORAGE_LEVELDB_UTIL_VLOG_WRITER_H_
#define STORAGE_LEVELDB_UTIL_VLOG_WRITER_H_

#include "leveldb/status.h"
#include "leveldb/env.h"

namespace leveldb {
class VlogWriter {
 public:
  explicit VlogWriter(WritableFile* dest, uint64_t offset);

  Status Add(Slice key, Slice value, std::string *handle);

  Status Sync();

  Status Close();

 private:
  WritableFile* dest_;
  uint64_t offset_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_VLOG_WRITER_H_
