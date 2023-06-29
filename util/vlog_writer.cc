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

class WriterHandler : public WriteBatch::Handler{
 public:
  explicit WriterHandler(VlogWriter* writer, WriteBatch* batch)
      :writer_(writer), batch_(batch){

  }

  ~WriterHandler() override = default;

  void Put(const Slice& key, const Slice& value) override {
    std::string handle;
    writer_->Add(key, value, &handle);
    batch_->Put(key, handle);
  }

  void Delete(const Slice& key) override {
    batch_->Delete(key);
  }

 private:
  VlogWriter* writer_;
  WriteBatch* batch_;
};

WriteBatch VlogWriter::InsertInto(const WriteBatch* b, VlogWriter* writer) {
  WriteBatch result;
  WriterHandler handler(writer, &result);
  b->Iterate(&handler);
  return result;
}

Status VlogWriter::Add(const Slice& key, const Slice& value, std::string* handle) {
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

VlogWriter* NewVlogWriter(const std::string& dbname) {
  WritableFile* vlog_file = nullptr;
  Status s = Env::Default()->NewAppendableFile(VlogFileName(dbname), &vlog_file);

  uint64_t file_size = 0;
  Env::Default()->GetFileSize(VlogFileName(dbname), &file_size);
  return new VlogWriter(vlog_file, file_size);
}
}  // namespace leveldb