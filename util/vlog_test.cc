//
// Created by WangTingZheng on 2023/6/27.
//
#include "leveldb/filter_policy.h"

#include "util/coding.h"

#include "arena.h"
#include "gtest/gtest.h"
#include "vlog_reader.h"
#include "vlog_writer.h"

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

class VlogTest : public testing::Test {
 public:
  VlogTest() : reader_(nullptr), source_(nullptr), sink_(new StringSink()) {
    writer_ = new VlogWriter(sink_, 0);
  }

  void Add(Slice key, Slice value, std::string* handle) {
    writer_->Add(key, value, handle);
  }

  void FinishAdd() {
    if (reader_ == nullptr) {
      source_ = new StringSource(sink_->contents());
      reader_ = new VlogReader(source_);
    }
  }

  void Reader(Slice* value, std::string handle) {
    FinishAdd();
    reader_->EncodingValue(handle, value, &arena);
  }

  ~VlogTest() override {
    delete writer_;
    delete reader_;
    delete source_;
    delete sink_;
  }

 private:
  VlogWriter* writer_;
  VlogReader* reader_;

  StringSource* source_;
  StringSink* sink_;

  Arena arena;
};

class VlogTestInFS : public testing::Test {
 public:
  VlogTestInFS() : reader_(nullptr),
                   source_(nullptr),sink_(nullptr) {
  }

  Status Init(){
    // get test dir path, windows has it's own file path kind
    std::string default_db_path;
    Env::Default()->GetTestDirectory(&default_db_path);
    dir_path_ = default_db_path + "/vlogtestinfs";
    file_path_ = dir_path_ + "/test_file";

    // create dir if not exists
    if(!Env::Default()->FileExists(dir_path_)){
      status_ = Env::Default()->CreateDir(dir_path_);
      if(!status_.ok()){
        return status_;
      }
    }

    // create files
    status_ = Env::Default()->NewAppendableFile(file_path_, &sink_);
    if(!status_.ok()){
      return status_;
    }
    uint64_t entry_size = 0;
    status_ = Env::Default()->GetFileSize(file_path_, &entry_size);
    if(status_.ok()){
      writer_ = new VlogWriter(sink_, entry_size);
    }

    return status_;
  }

  Status CleanFile(){
    // clean file
    Status s;
    if(Env::Default()->FileExists(file_path_)) {
      s = Env::Default()->DeleteFile(file_path_);
      if(s.ok()) {
        s = Env::Default()->RemoveDir(dir_path_);
      }
    }
    return s;
  }

  void Clear(){
    delete writer_;
    delete reader_;
    delete source_;
    delete sink_;

    writer_ = nullptr;
    reader_ = nullptr;
    source_ = nullptr;
    sink_ = nullptr;
  }

  void Add(Slice key, Slice value, std::string* handle) {
    writer_->Add(key, value, handle);
  }

  void FinishAdd() {
    if (reader_ == nullptr) {
      // sync and unlink file
      // open by reader
      sink_->Sync();
      delete sink_;
      sink_ = nullptr;

      status_ = Env::Default()->NewRandomAccessFile(file_path_, &source_);
      if(!status_.ok()){
        return;
      }
      reader_ = new VlogReader(source_);
    }
  }

  void Reader(Slice* value, std::string handle) {
    FinishAdd();
    reader_->EncodingValue(handle, value, &arena);
  }

  Status status() const {
    return status_;
  }

  ~VlogTestInFS() override{
    CleanFile();
    Clear();
  }

 private:
  VlogWriter* writer_;
  VlogReader* reader_;

  Status status_;
  std::string dir_path_;
  std::string file_path_;

  RandomAccessFile* source_;
  WritableFile* sink_;

  Arena arena;
};

TEST_F(VlogTest, Single) {
  std::string handle;
  Add("key", "value", &handle);

  FinishAdd();

  Slice value;
  Reader(&value, handle);
  ASSERT_EQ(value.ToString(), "value");
}

TEST_F(VlogTest, Multi) {
  std::vector<std::string> handles;
  int N = 1000;
  for (int i = 0; i < N; i++) {
    std::string handle;
    Add("key" + std::to_string(i), "value" + std::to_string(i), &handle);
    handles.push_back(handle);
  }

  FinishAdd();

  for (int i = 0; i < N; i++) {
    Slice value;
    std::string handle = handles[i];
    Reader(&value, handle);
    ASSERT_EQ(value.ToString(), "value" + std::to_string(i));
  }
}

TEST_F(VlogTestInFS, Single) {
  Init();
  ASSERT_TRUE(Status().ok());
  std::string handle;
  Add("key", "value", &handle);
  FinishAdd();
  ASSERT_TRUE(Status().ok());

  Slice value;
  Reader(&value, handle);
  ASSERT_EQ(value.ToString(), "value");
  Status s = CleanFile();
  ASSERT_EQ(s.ToString(), Status::OK().ToString());
}

TEST_F(VlogTestInFS, Multi) {
  Init();
  ASSERT_TRUE(Status().ok());

  std::vector<std::string> handles;
  int N = 1000;
  for (int i = 0; i < N; i++) {
    std::string handle;
    Add("key" + std::to_string(i), "value" + std::to_string(i), &handle);
    handles.push_back(handle);
  }

  FinishAdd();
  ASSERT_TRUE(Status().ok());

  for (int i = 0; i < N; i++) {
    Slice value;
    std::string handle = handles[i];
    Reader(&value, handle);
    ASSERT_EQ(value.ToString(), "value" + std::to_string(i));
  }
  Clear();
}

TEST_F(VlogTestInFS, ReWrite) {
  Init();
  ASSERT_TRUE(Status().ok());

  std::string handle;
  Add("key1", "value1", &handle);

  FinishAdd();
  ASSERT_TRUE(Status().ok());

  Clear();
  Init();

  std::string handle_rw;
  Add("key2", "value2", &handle_rw);

  FinishAdd();

  Slice value1;
  Reader(&value1, handle);
  ASSERT_EQ(value1.ToString(), "value1");

  Slice value2;
  Reader(&value2, handle_rw);
  ASSERT_EQ(value2.ToString(), "value2");

  CleanFile();
}
}  // namespace leveldb