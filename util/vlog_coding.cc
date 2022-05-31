//
// Created by 14037 on 2022/5/31.
//

#include "vlog_coding.h"
#include "crc32c.h"

namespace leveldb {
    namespace vlog{
        Status DecodeKV(Slice* src, std::string* key, std::string* value, ValueType *type) {
            if(src->size() < 1 + 4) return Status::Corruption("src is too short!");

            Slice tmp_key, tmp_value;
            *type = (ValueType) src->data()[0];
            if (*type != kTypeValue) return Status::Corruption("type not match");

            src->remove_prefix(1);
            if(!(GetLengthPrefixedSlice(src, &tmp_key) &&GetLengthPrefixedSlice(src, &tmp_value))){
                return Status::Corruption("key or value did not parse well");
            } else{
                *key = tmp_key.ToString();
                *value = tmp_value.ToString();
                return Status::OK();
            }
        }

        Status DecodeRecord(Slice src, Slice *kv) {
            if(src.size() < 4 + 8) return Status::Corruption("src is too short");

            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(src.data()));
            src.remove_prefix(4);
            uint64_t length = DecodeFixed64(src.data());
            src.remove_prefix(8);
            uint32_t actual_crc = crc32c::Value(src.data(), length);

            if(actual_crc == expected_crc){
                *kv = Slice(src.data(), length);
                return Status::OK();
            }
            else return Status::Corruption("crc check failed");
        }

        Status DecodeMeta(const std::string& addr, uint64_t *log_number, uint64_t *offset, uint64_t *size) {
            if(addr.size() < 24) return Status::NotFound("addr is too short");
            Slice input(addr);
            *log_number = DecodeFixed64(input.data());
            input.remove_prefix(8);
            *offset = DecodeFixed64(input.data());
            input.remove_prefix(8);
            *size = DecodeFixed64(input.data());

            return Status::OK();
        }

        std::string EncodeMeta(uint64_t log_number, uint64_t offset, uint64_t size) {
            std::string dst;
            PutFixed64(&dst, log_number);
            PutFixed64(&dst, offset);
            PutFixed64(&dst, size);

            return dst;
        }

        std::string EncodeKV(Slice key, Slice value) {
            std::string dst;
            dst.push_back(kTypeValue);
            PutLengthPrefixedSlice(&dst, key);
            PutLengthPrefixedSlice(&dst, value);

            return dst;
        }

        void EncodeRecord(Slice *result, Slice data){
            const char* ptr = data.data();
            size_t left = data.size();
            // 4 + 8
            char buf[kVHeaderMaxSize + left];

            // 放入4Byte的crc
            uint32_t crc = crc32c::Extend(0, ptr, left);
            crc = crc32c::Mask(crc);                 // Adjust for storage
            EncodeFixed32(buf, crc);
            EncodeFixed64(&buf[4], left);

            memcpy(&buf[kVHeaderMaxSize], ptr, left);

            Slice r(buf, kVHeaderMaxSize + left);

            *result = r;
        }
    }
} // leveldb