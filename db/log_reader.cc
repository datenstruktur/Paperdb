// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

// 跳过[0, initial_offset_]之前的
bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  // block_start_location: 离initial_offset_最近的Block的开头
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    // 从磁盘中读取一个record到fragment
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) { // 如果有initial_offset
      if (record_type == kMiddleType) { //
        continue;
      } else if (record_type == kLastType) {//
        resyncing_ = false;
        continue;           // 跳过
      } else {
        resyncing_ = false;
      }
    }

    // 对不同的分段进行处理
    switch (record_type) {
      case kFullType: // 如果是完整的，直接返回
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType: // 如果是第一个，创建一个临时变量来存储完整的数据
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size()); // 加入到这个临时变量中
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) { // 没有第一条
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size()); // 加入到临时变量中
        }
        break;

      case kLastType: // 如果是最后一条就合并
        if (!in_fragmented_record) { // 如果没有第一条
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size()); //加入到临时变量
          *record = Slice(*scratch); // 把临时变量赋值进record中
          last_record_offset_ = prospective_record_offset; //
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) { //之前读有分段的数据
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

// 读取下一条record到result
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {  // 如果buffer_的空间已经不够了，说明已经无法再读，就需要从磁盘中读取
      if (!eof_) { // 如果上次没有读取错误
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear(); //清除buffer_的空间
        // 读取一个Block到buffer_
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_); // 读取一个block大小到buffer_
        end_of_buffer_offset_ += buffer_.size(); //
        if (!status.ok()) { // 如果读取失败
          buffer_.clear();  // 清除已经读取的buffer_
          ReportDrop(kBlockSize, status); //
          eof_ = true; // 设置标志位
          return kEof;
        } else if (buffer_.size() < kBlockSize) { // 如果读取成功但是读取的Block的长度不够
          eof_ = true; // 说明读错了，下个循环清除数据跳出
        }
        continue;
        // 接下来的语句是肯定要跳出的
      } else { // 上次读取的buffer_的过程中失败了
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // 从buffer_中解析一条完整的record
    // Parse the header
    const char* header = buffer_.data();

    // 解析长度
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;    // 解析长度的前8位
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;    // 解析长度的后8位

    // 解析type
    const unsigned int type = header[6];
    // 计算出长度
    const uint32_t length = a | (b << 8);

    // 检查buffer_的长度
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear(); //清空block
      if (!eof_) { // 如果读取block没出错
        // 那么肯定是block里的结构出错了
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    // 检查type和数据的长度
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    // 检查crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) { // 如果校验失败
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // 清除这一条record
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    // (buffer_.size() + kHeaderSize + length): 解析buffer_之前的长度
    // end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length
    // end_of_buffer_offset_ - (buffer_.size() + kHeaderSize + length): 解析buffer_之前的偏移量
    // @todo 为什么不在之前就判断？
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) { // 如果读取的record在initial_offset_之前，说明这个record不需要
      result->clear(); //清除结果
      return kBadRecord;
    }

    // 返回数据
    *result = Slice(header + kHeaderSize, length);

    // 返回类型
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
