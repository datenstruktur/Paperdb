// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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
  FileImpl() : write_offset_(0) {
    sink_ = new StringSink();
    source_ = nullptr;
  }

  void WriteRawFilters(std::vector<std::string> filters, CompressionType type, BlockHandle* handle) {
    assert(!filters.empty());
    handle->set_offset(write_offset_);
    handle->set_size(filters[0].size());

    Status status;

    for(int i = 0; i < filters.size(); i++){
      std::string filter = filters[i];
      Slice filter_slice = Slice(filter);
      status = sink_->Append(filter_slice);
      if(status.ok()){
        char trailer[kBlockTrailerSize];
        trailer[0] = type;
        uint32_t crc = crc32c::Value(filter_slice.data(), filter_slice.size());
        crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        status = sink_->Append(Slice(trailer, kBlockTrailerSize));
        if (status.ok()) {
          write_offset_ += filter_slice.size() + kBlockTrailerSize;
        }
      }
    }
  }

  inline StringSource* GetSource() {
    if (source_ == nullptr) {
      source_ = new StringSource(sink_->contents());
    }
    return source_;
  }

  ~FileImpl() {
    delete sink_;
    delete source_;
  }

 private:
  StringSink* sink_;
  StringSource* source_;
  uint64_t write_offset_;
};

// For testing: emit an array with one hash value per key
class TestHashFilter : public FilterPolicy {
 public:
  const char* Name() const override { return "TestHashFilter"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst,
                    int index) const override {
    for (int i = 0; i < n; i++) {
      uint32_t h = Hash(keys[i].data(), keys[i].size(), index);
      PutFixed32(dst, h);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter,
                   int index) const override {
    uint32_t h = Hash(key.data(), key.size(), index);
    for (size_t i = 0; i + 4 <= filter.size(); i += 4) {
      if (h == DecodeFixed32(filter.data() + i)) {
        return true;
      }
    }
    return false;
  }
};

class FilterBlockTest : public testing::Test {
 public:
  TestHashFilter policy_;
};

TEST_F(FilterBlockTest, EmptyBuilder) {
  FilterBlockBuilder builder(&policy_);
  FileImpl file;
  BlockHandle handle;
  file.WriteRawFilters(builder.ReturnFilters(), kNoCompression, &handle);

  Slice block = builder.Finish(handle);
  char *filter_meta = (char *)malloc(sizeof (char) * block.size());
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  ASSERT_EQ(
      "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
      "\\x00\\x00\\x00\\x00"
      "\\x01\\x00\\x00\\x00"
      "\\x04\\x00\\x00\\x00"
      "\\x0b",
      EscapeString(block));

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);
  ASSERT_TRUE(reader.KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(100000, "foo"));

  delete filter_meta;
}

TEST_F(FilterBlockTest, SingleChunk) {
  FilterBlockBuilder builder(&policy_);
  builder.StartBlock(100);
  builder.AddKey("foo");
  builder.AddKey("bar");
  builder.AddKey("box");
  builder.StartBlock(200);
  builder.AddKey("box");
  builder.StartBlock(300);
  builder.AddKey("hello");

  FileImpl file;
  BlockHandle handle;
  const std::vector<std::string>& filters = builder.ReturnFilters();
  file.WriteRawFilters(filters, kNoCompression, &handle);
  Slice block = builder.Finish(handle);

  char *filter_meta = (char *)malloc(sizeof(char) * block.size());
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  std::string escapestring = EscapeString(block);
  escapestring = escapestring.substr(escapestring.size() - 21 * 4, 21 * 4);

  ASSERT_EQ(
      "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
      "\\x14\\x00\\x00\\x00"
      "\\x01\\x00\\x00\\x00"
      "\\x04\\x00\\x00\\x00"
      "\\x0b",
      escapestring);

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);

  ASSERT_TRUE(reader.KeyMayMatch(100, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "bar"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "box"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "hello"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(100, "missing"));
  ASSERT_TRUE(!reader.KeyMayMatch(100, "other"));

  delete filter_meta;
}

TEST_F(FilterBlockTest, MultiChunk) {
  FilterBlockBuilder builder(&policy_);

  // First filter
  builder.StartBlock(0);
  builder.AddKey("foo");
  builder.StartBlock(2000);
  builder.AddKey("bar");

  // Second filter
  builder.StartBlock(3100);
  builder.AddKey("box");

  // Third filter is empty

  // Last filter
  builder.StartBlock(9000);
  builder.AddKey("box");
  builder.AddKey("hello");

  // create reader
  FileImpl file;
  const std::vector<std::string>& filter = builder.ReturnFilters();
  BlockHandle handle;
  file.WriteRawFilters(filter, kNoCompression, &handle);
  Slice block = builder.Finish(handle);

  char *filter_meta = (char *)malloc(sizeof (char ) * block.size());
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);

  // Check first filter
  ASSERT_TRUE(reader.KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(2000, "bar"));
  ASSERT_TRUE(!reader.KeyMayMatch(0, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(0, "hello"));

  // Check second filter
  ASSERT_TRUE(reader.KeyMayMatch(3100, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(3100, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(3100, "bar"));
  ASSERT_TRUE(!reader.KeyMayMatch(3100, "hello"));

  // Check third filter (empty)
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "bar"));
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "hello"));

  // Check last filter
  ASSERT_TRUE(reader.KeyMayMatch(9000, "box"));
  ASSERT_TRUE(reader.KeyMayMatch(9000, "hello"));
  ASSERT_TRUE(!reader.KeyMayMatch(9000, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(9000, "bar"));

  delete filter_meta;
}

TEST_F(FilterBlockTest, LoadAndExcit){
  FilterBlockBuilder builder(&policy_);

  // First filter
  builder.StartBlock(0);
  builder.AddKey("foo");
  builder.StartBlock(2000);
  builder.AddKey("bar");

  // Second filter
  builder.StartBlock(3100);
  builder.AddKey("box");

  // Third filter is empty

  // Last filter
  builder.StartBlock(9000);
  builder.AddKey("box");
  builder.AddKey("hello");

  FileImpl file;
  const std::vector<std::string>& filter = builder.ReturnFilters();
  BlockHandle handle;
  file.WriteRawFilters(filter, kNoCompression, &handle);
  Slice block = builder.Finish(handle);

  char *filter_meta = (char *)malloc(sizeof (char ) * block.size());
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);

  // todo can automatically adapt to different parameters
  ASSERT_EQ(reader.FilterUnitsNumber(), 1);
  ASSERT_TRUE(reader.EvictFilter().ok());
  ASSERT_EQ(reader.FilterUnitsNumber(), 0);
  ASSERT_FALSE(reader.EvictFilter().ok());

  ASSERT_TRUE(reader.LoadFilter().ok());
  ASSERT_EQ(reader.FilterUnitsNumber(), 1);

  ASSERT_TRUE(reader.LoadFilter().ok());
  ASSERT_EQ(reader.FilterUnitsNumber(), 2);

  ASSERT_TRUE(reader.LoadFilter().ok());
  ASSERT_EQ(reader.FilterUnitsNumber(), 3);

  ASSERT_TRUE(reader.LoadFilter().ok());
  ASSERT_EQ(reader.FilterUnitsNumber(), 4);

  ASSERT_FALSE(reader.LoadFilter().ok());

  delete filter_meta;
}

}  // namespace leveldb
