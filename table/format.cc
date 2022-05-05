// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);

  //sizeof(metaindex_handle_ + index_handle_) <= 2 * BlockHandle::kMaxEncodedLength
  // resize() 扩充成2 * BlockHandle::kMaxEncodedLength，就完成了Padding
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding

  //PutFixed32只能编码32比特的数据，而kTableMagicNumber有64位，8Byte
  //0xffff,ffff为8个十六进制f，一个f为4比特，总共8*4 = 32比特
  // kTableMagicNumber & 0xffff,ffff为取前32比特
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  //取后32比特写入
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));

  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

//
Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

/*
 * handle:
 * result:
 */
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size()); // Block长度
  char* buf = new char[n + kBlockTrailerSize]; // 加上type(什么类型的压缩)和crc校验码的长度
  Slice contents;
  // 从handle的offset处读取一个Block长度的数据到contents中
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    // 从type后面开始读出crc
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1); //计算Block中[0, n + 1]的crc
    if (actual != crc) { // 如果校验失败
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  // 读取type
  switch (data[n]) {
    case kNoCompression: //如果不需要压缩
      // PosixMmapReadableFile中的Read函数不会将data里的数据读取到buf
      // data 指向一块内存区域，这块内存是磁盘文件在内存中的映射，由OS管理，所以我们就不需要buf了
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n); //读取data到result
        result->heap_allocated = false; // 不需要Block管理Block数据，由OS管理，Block对象删除了也不会删除它
        result->cachable = false;  // Do not double-cache，已经在内存中了，没必要用LRUCache中缓存它
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0; // 解压缩后的长度
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) { // 获得解压缩后的长度
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) { // 解压缩到ubuf
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength); // 把解压缩后的数据赋值到result内
      result->heap_allocated = true;
      result->cachable = true; //可以被插入到LRUCache
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
