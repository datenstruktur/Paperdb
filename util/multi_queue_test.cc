// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/multi_queue.h"

#include <vector>
#include "file_impl.h"
#include "gtest/gtest.h"

namespace leveldb {
class MultiQueueTest : public testing::Test {
 public:
  MultiQueueTest(){
    multi_queue_ = NewMultiQueue();
    policy_ = NewBloomFilterPolicy(10);
  }

  ~MultiQueueTest(){
    delete multi_queue_;
    delete policy_;
    for(int i = 0; i < file_impl_.size(); i++){
      delete file_impl_[i];
    }
  }

  FilterBlockReader* NewReader(){
    FilterBlockBuilder builder(policy_);
    builder.StartBlock(100);
    builder.AddKey("foo");

    FileImpl *file = new FileImpl();
    BlockHandle handle;
    const std::vector<std::string>& filters = builder.ReturnFilters();
    file->WriteRawFilters(filters, &handle);

    Slice block = builder.Finish(handle);

    char *filter_meta = (char *)malloc(sizeof(char) * block.size());
    memcpy(filter_meta, block.data(), block.size());
    Slice filter_meta_data(filter_meta, block.size());

    StringSource* source = file->GetSource();
    file_impl_.push_back(file);
    return new FilterBlockReader(policy_, filter_meta_data, source);
  }

  static void Deleter(const Slice& key, FilterBlockReader* reader) {
    delete reader;
  }

  MultiQueue::Handle* Insert(const Slice& key){
    return multi_queue_->Insert(key, NewReader(), MultiQueueTest::Deleter);
  }

  MultiQueue::Handle* Lookup(const Slice& key){
    return multi_queue_->Lookup(key);
  }

  void Erase(const Slice& key){
    multi_queue_->Erase(key);
  }

  FilterBlockReader* Value(MultiQueue::Handle* handle){
    return multi_queue_->Value(handle);
  }

  size_t TotalCharge(){
    return multi_queue_->TotalCharge();
  }

  MultiQueue *multi_queue_;
  const FilterPolicy *policy_;
  std::vector<FileImpl*> file_impl_;
};

TEST_F(MultiQueueTest, InsertAndLookup){
    MultiQueue::Handle* insert_handle = Insert("key1");
    ASSERT_NE(insert_handle, nullptr);
    MultiQueue::Handle* lookup_handle = Lookup("key1");
    ASSERT_NE(lookup_handle, nullptr);

    ASSERT_EQ(lookup_handle, insert_handle);

    FilterBlockReader* reader = Value(lookup_handle);
    ASSERT_NE(reader, nullptr);

    ASSERT_TRUE(reader->KeyMayMatch(100, "foo"));
}

TEST_F(MultiQueueTest, InsertAndErase){
    MultiQueue::Handle* insert_handle = Insert("key1");
    ASSERT_NE(insert_handle, nullptr);
    Erase("key1");

    MultiQueue::Handle* lookup_handle = Lookup("key1");
    ASSERT_EQ(lookup_handle, nullptr);
}

TEST_F(MultiQueueTest, TotalCharge){
    MultiQueue::Handle* insert_handle = Insert("key1");
    ASSERT_NE(insert_handle, nullptr);
    FilterBlockReader* reader = Value(insert_handle);
    ASSERT_EQ(TotalCharge(), reader->Size());
    Erase("key1");
    ASSERT_EQ(TotalCharge(), 0);
}

TEST_F(MultiQueueTest, NewId) {
    uint64_t a = multi_queue_->NewId();
    uint64_t b = multi_queue_->NewId();
    ASSERT_NE(a, b);
}
}  // namespace leveldb
