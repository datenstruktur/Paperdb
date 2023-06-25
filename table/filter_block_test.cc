// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"

#include "util/file_impl.h"

#include "gtest/gtest.h"

namespace leveldb {
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

  double FalsePositiveRate() const override { return 0; }
};

class FilterBlockTest : public testing::Test {
 public:
  TestHashFilter policy_;
};

TEST_F(FilterBlockTest, EmptyBuilder) {
  FilterBlockBuilder builder(&policy_);
  FileImpl file;
  BlockHandle handle;
  file.WriteRawFilters(builder.ReturnFilters(), &handle);

  Slice block = builder.Finish(handle);
  char* filter_meta = new char[block.size()];
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  ASSERT_EQ(
      "\\x00\\x00\\x00\\x00"                      // bitmap len
      "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"  // offset
      "\\x00\\x00\\x00\\x00"                      // size
      "\\x0" +
          std::to_string(loaded_filters_number) +
          "\\x00\\x00\\x00"  // loaded
          "\\x0" +
          std::to_string(filters_number) +
          "\\x00\\x00\\x00"  // number
          "\\x0b",           // baselg
      EscapeString(block));

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);
  ASSERT_TRUE(reader.KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(100000, "foo"));
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
  file.WriteRawFilters(filters, &handle);
  Slice block = builder.Finish(handle);

  char* filter_meta = new char[block.size()];
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  std::string escapestring = EscapeString(block);
  // remove filter's bitmap
  escapestring = escapestring.substr(escapestring.size() - 25 * 4, 25 * 4);

  ASSERT_EQ(
      "\\x14\\x00\\x00\\x00"  // bitmap len(20 = 4Byte * 5 key)
      "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"  // bitmap offset(0)
      // bitmap size in disk, as same as bitmap len
      "\\x14\\x00\\x00\\x00"
      "\\x0" +
          std::to_string(loaded_filters_number) +
          "\\x00\\x00\\x00"
          "\\x0" +
          std::to_string(filters_number) +
          "\\x00\\x00\\x00"
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
  file.WriteRawFilters(filter, &handle);
  Slice block = builder.Finish(handle);

  char* filter_meta = new char[block.size()];
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
}

TEST_F(FilterBlockTest, LoadAndExcit) {
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
  file.WriteRawFilters(filter, &handle);
  Slice block = builder.Finish(handle);

  char* filter_meta = new char[block.size()];
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);

  // todo can automatically adapt to different parameters
  for (int i = loaded_filters_number; i > 0; i--) {
    ASSERT_EQ(reader.FilterUnitsNumber(), i);
    ASSERT_TRUE(reader.EvictFilter().ok());
  }

  ASSERT_FALSE(reader.EvictFilter().ok());

  for (int i = 0; i < filters_number; i++) {
    ASSERT_EQ(reader.FilterUnitsNumber(), i);
    ASSERT_TRUE(reader.LoadFilter().ok());
  }

  ASSERT_FALSE(reader.LoadFilter().ok());
}

TEST_F(FilterBlockTest, Hotness) {
  // to support internal key
  InternalFilterPolicy policy(&policy_);
  FilterBlockBuilder builder(&policy);

  // First filter
  builder.StartBlock(0);
  ParsedInternalKey add_key("foo", 1, kTypeValue);
  std::string add_result;
  AppendInternalKey(&add_result, add_key);
  builder.AddKey(add_result);

  // write bitmap, create builder
  FileImpl file;
  BlockHandle handle;
  const std::vector<std::string>& filters = builder.ReturnFilters();
  file.WriteRawFilters(filters, &handle);
  Slice block = builder.Finish(handle);

  // create reader
  char* filter_meta = new char[block.size()];
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());
  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy, filter_meta_data, source);

  // check
  for (uint64_t sn = 1; sn < 30000; sn++) {
    ParsedInternalKey check_key("foo", sn, kTypeValue);
    std::string check_result;
    AppendInternalKey(&check_result, check_key);
    // sequence number is sn
    ASSERT_TRUE(reader.KeyMayMatch(0, check_result));
    ASSERT_EQ(reader.AccessTime(), sn);

    // reader died in sn + 30000
    ASSERT_FALSE(reader.IsCold(30000 + sn - 1));
    ASSERT_TRUE(reader.IsCold(30000 + sn));
  }
}

TEST_F(FilterBlockTest, Size) {
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
  file.WriteRawFilters(filters, &handle);

  size_t bitmap_size = handle.size();
  Slice block = builder.Finish(handle);

  char* filter_meta = new char[block.size()];
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  StringSource* source = file.GetSource();
  FilterBlockReader reader(&policy_, filter_meta_data, source);

  // evict all filter units
  while (reader.EvictFilter().ok()) {
  }
  ASSERT_EQ(reader.FilterUnitsNumber(), 0);
  ASSERT_EQ(reader.Size(), 0);

  // load filter units one by one
  // check memory overhead
  int filter_unit_number = 1;
  while (reader.LoadFilter().ok()) {
    ASSERT_EQ(reader.FilterUnitsNumber(), filter_unit_number);
    ASSERT_EQ(reader.Size(), bitmap_size * filter_unit_number);
    filter_unit_number++;
  }
}

TEST_F(FilterBlockTest, IOs) {
  const FilterPolicy* policy = NewBloomFilterPolicy(10);
  FilterBlockBuilder builder(policy);
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
  file.WriteRawFilters(filters, &handle);

  Slice block = builder.Finish(handle);

  char* filter_meta = new char[block.size()];
  memcpy(filter_meta, block.data(), block.size());
  Slice filter_meta_data(filter_meta, block.size());

  StringSource* source = file.GetSource();
  FilterBlockReader reader(policy, filter_meta_data, source);

  int access = 1000;
  for (int i = 0; i < access; i++) {
    ASSERT_TRUE(reader.KeyMayMatch(100, "foo"));
  }

  double false_positive_rate = pow(0.6185, 10);

  ASSERT_EQ(reader.AccessTime(), access);
  ASSERT_EQ(reader.IOs(),
            pow(false_positive_rate, loaded_filters_number) * access);

  if (loaded_filters_number < filters_number) {
    ASSERT_EQ(reader.LoadIOs(),
              pow(false_positive_rate, loaded_filters_number + 1) * access);
  }

  if (loaded_filters_number > 1) {
    ASSERT_EQ(reader.EvictIOs(),
              pow(false_positive_rate, loaded_filters_number - 1) * access);
  }

  delete policy;
}

}  // namespace leveldb
