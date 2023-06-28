// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "util/arena.h"
#include "util/vlog_reader.h"

namespace leveldb {

struct BlockContents;
class Comparator;
class VlogReader;
class Arena;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents, VlogReader* vlog_reader = nullptr);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;
  class VlogIterator;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]

  Arena arena_;
  VlogReader* vlog_reader_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
