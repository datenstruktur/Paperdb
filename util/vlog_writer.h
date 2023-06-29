//
// Created by WangTingZheng on 2023/6/27.
//

#ifndef STORAGE_LEVELDB_UTIL_VLOG_WRITER_H_
#define STORAGE_LEVELDB_UTIL_VLOG_WRITER_H_

#include "db/filename.h"

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

namespace leveldb {
class VlogWriter {
 public:
  explicit VlogWriter(WritableFile* dest, uint64_t offset);

  static WriteBatch InsertInto(const WriteBatch* b, VlogWriter* writer);

  ~VlogWriter(){ delete dest_;}

  Status Add(const Slice& key, const Slice& value, std::string *handle);

  Status Sync();

  Status Close();

 private:
  WritableFile* dest_;
  uint64_t offset_;
};

VlogWriter* NewVlogWriter(const std::string& dbname);
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_VLOG_WRITER_H_
