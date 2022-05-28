//
// Created by 14037 on 2022/5/27.
//

#include <atomic>
#include <iostream>
#include "leveldb/env.h"
#include "db/filename.h"
#include "db/vlog_writer.h"
#include "db/vlog_reader.h"
#include "db/write_batch_internal.h"
#include "gtest/gtest.h"

namespace leveldb {
    class VlogTest : public testing::Test {
        public:
            VlogTest(uint64_t log_number, const std::string &dbname)
                    : log_number_(log_number),
                      dbname_(dbname),
                      env_(Env::Default()),
                      src_batch_(new WriteBatch()),
                      tmp_batch_(new WriteBatch()),
                      dst_batch_(new WriteBatch()),
                      reader_(nullptr) {
                WritableFile *dest;
                env_->NewWritableFile(LogFileName(dbname, log_number), &dest);
                writer_ = new vlog::VlogWriter(dest);
            }

            VlogTest()
                    : log_number_(1),
                      dbname_("dbname"),
                      env_(Env::Default()),
                      src_batch_(new WriteBatch()),
                      tmp_batch_(new WriteBatch()),
                      dst_batch_(new WriteBatch()),
                      reader_(nullptr) {
                WritableFile *dest;
                env_->NewWritableFile(LogFileName("dbname", 1), &dest);
                writer_ = new vlog::VlogWriter(dest);
            }

            void Put(const Slice &key, const Slice &value) {
                src_batch_->Put(key, value);
            }

            void Delete(const Slice &key) {
                src_batch_->Delete(key);
            }

            Status SyncBatch() {
                assert(src_batch_ != nullptr);
                assert(writer_ != nullptr);

                Status status = writer_->AddRecord(log_number_, src_batch_, tmp_batch_);
                if (!status.ok()) return status;

                status = writer_->Sync();
                if (!status.ok()) return status;

                return Status::OK();
            }

            Status ReadBatch() {
                assert(dst_batch_ != nullptr);
                dst_batch_->Clear();
                uint64_t pos = 0;
                Slice key, value;
                ValueType type;

                Status status;
                Slice get_value, kv;

                std::string real_key, real_value;
                ValueType real_type;

                while (tmp_batch_->ParseBatch(&pos, &key, &value, &type).ok()) {
                    if (type == leveldb::kTypeValue) {
                        uint64_t get_log_number, get_offset, get_size;
                        status = vlog::VlogWriter::DecodeMeta(value.ToString(), &get_log_number, &get_offset, &get_size);
                        if (!status.ok()) return status;

                        reader_ = new vlog::VlogReader(dbname_, get_log_number);
                        status = reader_->Read(get_offset, get_size, &get_value);
                        if (!status.ok()) return status;

                        status = vlog::VlogReader::DecodeRecord(get_value, &kv);
                        if (!status.ok()) return status;

                        status = vlog::VlogReader::DecodeKV(kv, &real_key, &real_value, &real_type);
                        if (!status.ok()) return status;

                        if (type != real_type || key.compare(real_key))
                            return Status::Corruption("read key or type is not match");

                        dst_batch_->Put(real_key, real_value);
                    } else if (type == leveldb::kTypeDeletion) {
                        dst_batch_->Delete(key);
                    } else {
                        return Status::Corruption("get error type");
                    }
                }

                return Status::OK();
            }

            bool CheckBatch() {
                assert(src_batch_ != nullptr);
                assert(dst_batch_ != nullptr);

                if (WriteBatchInternal::Count(src_batch_) != WriteBatchInternal::Count(dst_batch_)) return false;

                uint64_t src_pos = 0;
                uint64_t dst_pos = 0;

                Slice src_key, src_value;
                ValueType src_type;

                Slice dst_key, dst_value;
                ValueType dst_type;

                while ((src_batch_->ParseBatch(&src_pos, &src_key, &src_value, &src_type).ok() &&
                        dst_batch_->ParseBatch(&dst_pos, &dst_key, &dst_value, &dst_type).ok())) {
                    if (src_type != dst_type || src_key.compare(dst_key) != 0) return false;

                    if (src_type == leveldb::kTypeValue) {
                        if (src_value.compare(dst_value) != 0) return false;
                    }
                }

                return true;
            }

            ~VlogTest() override {
                env_->RemoveFile(LogFileName(dbname_, log_number_));
            }

    private:
        uint64_t log_number_;
        std::string dbname_;
        vlog::VlogWriter *writer_;
        vlog::VlogReader *reader_;
        Env *env_;

        WriteBatch *src_batch_;
        WriteBatch *tmp_batch_;
        WriteBatch *dst_batch_;
    };

    TEST_F(VlogTest, TestPutAndDelete) {
        Put("key1", "value1");
        Put("key2", "value2");
        Put("key3", "value3");
        Put("key4", "value4");
        Delete("key5");
        Put("key6", "value6");
        Delete("key7");
        Delete("key8");
        Put("key9", "value9");
        Delete("key10");
        Put("key11", "value11");

        ASSERT_TRUE(SyncBatch().ok());
        ASSERT_TRUE(ReadBatch().ok());
        ASSERT_TRUE(CheckBatch());
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}