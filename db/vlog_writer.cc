//
// Created by 14037 on 2022/5/25.
//

#include <iostream>
#include "vlog_writer.h"
#include "util/coding.h"
#include "dbformat.h"
#include "util/crc32c.h"
#include "write_batch_internal.h"

namespace leveldb {
    namespace vlog{

        Status VlogWriter::DecodeMeta(std::string addr, uint64_t *log_number, uint64_t *offset, uint64_t *size) {
            if(addr.size() < 24) return Status::NotFound("addr is too short");
            Slice input(addr);
            *log_number = DecodeFixed64(input.data());
            input.remove_prefix(8);
            *offset = DecodeFixed64(input.data());
            input.remove_prefix(8);
            *size = DecodeFixed64(input.data());

            return Status::OK();
        }

        Status VlogWriter::AddRecord(uint64_t log_number, WriteBatch *src_batch, WriteBatch *meta_batch) {
            Slice key, value;
            ValueType type;

            assert(src_batch != nullptr);
            assert(meta_batch != nullptr);
            assert(WriteBatchInternal::Count(meta_batch) == 0);

            uint64_t pos = 0;

            while (src_batch->ParseBatch(&pos, &key, &value, &type).ok()){
                switch (type) {
                    case kTypeValue:{
                        Slice record;
                        uint64_t offset, size;
                        record = EncodeKV(key, value);
                        offset = head_;
                        size = kVHeaderMaxSize + record.size();
                        Write(record);
                        meta_batch->Put(key, EncodeMeta(log_number, offset, size));
                        head_ += size;
                        break;
                    }
                    case kTypeDeletion:{
                        meta_batch->Delete(key);
                        break;
                    }
                    default:{
                        return Status::Corruption("bad entry in batch");
                    }
                }
            }
            if (WriteBatchInternal::Count(src_batch) != WriteBatchInternal::Count(meta_batch))
                return Status::Corruption("two batch has different kv count");

            SequenceNumber sn = WriteBatchInternal::Sequence(src_batch);
            WriteBatchInternal::SetSequence(meta_batch, sn);
            return Status::OK();
        }

        Status VlogWriter::Write(Slice data) {
            const char* ptr = data.data();
            size_t left = data.size();

            // 4 + 8
            char buf[kVHeaderMaxSize];

            // 放入4Byte的crc
            uint32_t crc = crc32c::Extend(0, ptr, left);
            crc = crc32c::Mask(crc);                 // Adjust for storage
            EncodeFixed32(buf, crc);
            EncodeFixed64(&buf[4], left);
            Status s = dest_->Append(Slice(buf, kVHeaderMaxSize));
            if (s.ok()) {
                s = dest_->Append(Slice(ptr, left));//写一条物理记录就刷一次
                if (s.ok()) {
                    s = dest_->Flush();
                }
            }
            return s;
        }

        std::string VlogWriter::EncodeMeta(uint64_t log_number, uint64_t offset, uint64_t size) {
            std::string dst;
            PutFixed64(&dst, log_number);
            PutFixed64(&dst, offset);
            PutFixed64(&dst, size);

            return dst;
        }

        std::string VlogWriter::EncodeKV(Slice key, Slice value) {
            std::string dst;
            dst.push_back(kTypeValue);
            PutLengthPrefixedSlice(&dst, key);
            PutLengthPrefixedSlice(&dst, value);

            return dst;
        }

        Status VlogWriter::Sync() {
            return dest_->Sync();
        }
    }
} // leveldb