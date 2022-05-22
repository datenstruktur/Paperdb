// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {  //装不下header
      // Switch to a new block
      if (leftover > 0) { // 装不下header但是还是有空间
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        // 用\x00填满
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 本block中够装data的空间
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 如果left < avail，说明可以装下，那么长度就是data的长度
    // 如果left >= avail， 说明装不下，那么长度就是剩下的长度avail
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // left < avail时，fragment_length == left，end等于true
    const bool end = (left == fragment_length);
    if (begin && end) { // 如果是第一个分段且本block可以装下
      type = kFullType;
    } else if (begin) { // 如果是第一个分段但是本block不能装下
      type = kFirstType; // 那么次分段是第一个分段
    } else if (end) { // 如果不是第一个分段，但是本block可以装下
      type = kLastType; // 那么它肯定是最后一个分段
    } else { // 如果既不是第一个分段本block又不能装下
      type = kMiddleType; // 那么就是中间的分段
    }

    // 从数据中截取fragment_length以持久化
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

// 写入一条记录
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
  char buf[kHeaderSize];
  // 前8位先取出来
  buf[4] = static_cast<char>(length & 0xff); // length (1 bytes)
  // 后8位取出来
  buf[5] = static_cast<char>(length >> 8); // length (1 bytes)
  buf[6] = static_cast<char>(t); // type (1 bytes)

  // Compute the crc of the record type and the payload.
  // 计算出[ptr, ptr + length]的crc值
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  // 为了存储，调整crc
  crc = crc32c::Mask(crc);  // Adjust for storage
  // 把crc以32位的形式编码到buf[0, 3]中
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 添加buf中head的部分
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) { // 如果添加成功
    // 添加
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
