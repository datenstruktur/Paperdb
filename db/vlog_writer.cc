//
// Created by 14037 on 2022/5/25.
//

#include <iostream>
#include "vlog_writer.h"
#include "util/coding.h"
#include "dbformat.h"
#include "util/crc32c.h"
#include "write_batch_internal.h"
#include "util/vlog_coding.h"

namespace leveldb {
    namespace vlog{
        Status VlogWriter::AddRecord(uint64_t log_number, WriteBatch *src_batch, WriteBatch *meta_batch) {
            Slice key, value;
            ValueType type;

            assert(src_batch != nullptr);
            assert(meta_batch != nullptr);
            assert(WriteBatchInternal::Count(meta_batch) == 0);

            uint64_t pos = 0;
            SequenceNumber sn = WriteBatchInternal::Sequence(src_batch);
            SequenceNumber first_sn = sn;

            while (src_batch->ParseBatch(&pos, &key, &value, &type).ok()){
                switch (type) {
                    case kTypeValue:{
                        std::string record;
                        uint64_t offset, size;
                        record = EncodeKV(sn, key, value);
                        sn ++;
                        offset = head_;
                        size = kVHeaderMaxSize + record.size();
                        Write(record);
                        meta_batch->Put(key, EncodeMeta(log_number, offset, size));
                        head_ += size;
                        break;
                    }
                    case kTypeDeletion:{
                        meta_batch->Delete(key);
                        sn++;
                        break;
                    }
                    default:{
                        return Status::Corruption("bad entry in batch");
                    }
                }
            }
            if (WriteBatchInternal::Count(src_batch) != WriteBatchInternal::Count(meta_batch))
                return Status::Corruption("two batch has different kv count");

            if(sn - first_sn != WriteBatchInternal::Count(src_batch)) return Status::Corruption("not read all kvs");
            WriteBatchInternal::SetSequence(meta_batch, first_sn);
            return Status::OK();
        }

        Status VlogWriter::Write(Slice data) {
            std::string output = EncodeRecord(data);
            Status s = dest_->Append(output);//写一条物理记录就刷一次
            if (s.ok()) {
                s = dest_->Flush();
            }
            return s;
        }

        Status VlogWriter::Sync() {
            return dest_->Sync();
        }

    }
} // leveldb