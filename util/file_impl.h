//
// Created by WangTingZheng on 2023/6/19.
//

#ifndef LEVELDB_FILE_IMPL_H
#define LEVELDB_FILE_IMPL_H

#include "table/filter_block.h"
#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/hash.h"
#include "util/logging.h"
#include "gtest/gtest.h"

namespace leveldb {

class StringSink : public WritableFile {
 public:
  ~StringSink() override = default;

  const std::string& contents() const { return contents_; }

  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }

  Status Append(const Slice& data) override {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice& contents)
      : contents_(contents.data(), contents.size()) {}

  ~StringSource() override = default;

  uint64_t Size() const { return contents_.size(); }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset >= contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - offset;
    }
    std::memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
};

class FileImpl {
 public:
  FileImpl();

  void WriteRawFilters(std::vector<std::string> filters, BlockHandle* handle);

  StringSource* GetSource();

  ~FileImpl();

 private:
  StringSink* sink_;
  StringSource* source_;
  uint64_t write_offset_;
};

}  // namespace leveldb

#endif  // LEVELDB_FILE_IMPL_H
