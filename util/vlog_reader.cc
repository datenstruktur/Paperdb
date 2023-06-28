//
// Created by WangTingZheng on 2023/6/27.
//

#include "vlog_reader.h"
#include "leveldb/env.h"
#include "table/format.h"
#include "coding.h"

namespace leveldb {

VlogReader::VlogReader(RandomAccessFile* file):file_(file){}

bool VlogReader::GetValueSize(Slice handle, uint64_t* value_size) {
  BlockHandle block_handle;
  if(!block_handle.DecodeFrom(&handle).ok()){
    return false;
  }

  *value_size = block_handle.size();
  return true;
}

void DecodingEntry(Slice *entry){
  uint32_t key_size = 0;
  GetVarint32(entry, &key_size);
  uint32_t value_size = 0;
  GetVarint32(entry, &value_size);
  *entry = Slice(entry->data() + key_size, value_size);
}

bool VlogReader::ReadRecond(Slice handle, Slice* value, char* buf, uint64_t entry_size) {
  BlockHandle block_handle;
  if(!block_handle.DecodeFrom(&handle).ok()){
    return false;
  }

  size_t offset = block_handle.offset();
  size_t size   = block_handle.size();

  if(entry_size != size){
    return false;
  }

  Status status = file_->Read(offset, size, value, buf);
  if(!status.ok()){
    return false;
  }

  DecodingEntry(value);
  return true;
}
}  // namespace leveldb