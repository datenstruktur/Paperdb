//
// Created by 14037 on 2022/5/25.
//

#include <iostream>
#include "vlog_reader.h"
#include "db/filename.h"
#include "vlog_writer.h"
#include "util/crc32c.h"

namespace leveldb {
    namespace vlog{

        VlogReader::VlogReader(std::string dbname, uint64_t log_number) {
            std::string path = LogFileName(dbname, log_number);
            Env *env = Env::Default();
            env->NewSequentialFile(path, &file_);
        }

        Status VlogReader::Read(uint64_t offset, uint64_t size, Slice *value) {
            if(!JumpTo(offset)){
                return Status::Corruption("Jump to offset failed");
            }

            char *buf = new char[size];
            Status status = file_->Read(size, value, buf);
            if (!status.ok()) return status;
            return Status::OK();
        }

        bool VlogReader::JumpTo(uint64_t offset) {
            if (offset > 0) {  //跳到距file文件头偏移pos的地方
                Status skip_status = file_->Jump(offset);
                if (!skip_status.ok()) {
                    return false;
                }
            }
            return true;
        }

        Status VlogReader::DecodeKV(Slice src, std::string* key, std::string* value, ValueType *type) {
            if(src.size() < 1 + 4) return Status::Corruption("src is too short!");

            Slice input(src);
            Slice tmp_key, tmp_value;
            *type = static_cast<ValueType>(input[0]);
            assert(*type == kTypeValue);
            input.remove_prefix(1);
            if(!(GetLengthPrefixedSlice(&input, &tmp_key) &&GetLengthPrefixedSlice(&input, &tmp_value))){
                return Status::Corruption("key or value did not parse well");
            } else{
                *key = tmp_key.ToString();
                *value = tmp_value.ToString();
                return Status::OK();
            }
        }

        Status VlogReader::DecodeRecord(Slice src, Slice *kv) {
            // check crc
            // get length
            // read kv

            const char *buf = src.data();
            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(buf));
            src.remove_prefix(4);
            uint64_t length = DecodeFixed64(src.data());
            src.remove_prefix(8);

            *kv = Slice(src.data(), length);
            uint32_t actual_crc = crc32c::Extend(0, kv->data(), length);
            if(actual_crc == expected_crc) return Status::OK();
            else {
                std::cout << src.data() << std::endl;
                return Status::Corruption("crc check failed");
            }
        }
    }
} // leveldb