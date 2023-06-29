//
// Created by WangTingZheng on 2023/6/27.
//

#include "vlog_writer.h"

#include "leveldb/env.h"
#include "table/format.h"
#include "coding.h"

namespace leveldb {

std::string DecodeEntry(Slice key, Slice value){
  std::string buffer;

  PutVarint32(&buffer, key.size());
  PutVarint32(&buffer, value.size());

  buffer.append(key.data(), key.size());
  buffer.append(value.data(), value.size());

  return buffer;
}

VlogWriter::VlogWriter(WritableFile* dest, uint64_t offset)
    :dest_(dest),offset_(offset) {}

Status VlogWriter::Add(Slice key, Slice value, std::string* handle) {
  std::string entry = DecodeEntry(key, value);

  BlockHandle block_handle;
  block_handle.set_offset(offset_);
  block_handle.set_size(entry.size());

  Status s = dest_->Append(entry);

  if(!s.ok()){
    return s;
  }

  offset_ += entry.size();
  handle->clear();
  block_handle.EncodeTo(handle);

  return s;
}

Status VlogWriter::Sync() {
  return dest_->Sync();
}

Status VlogWriter::Close() {
  return dest_->Close();
}
}  // namespace leveldb