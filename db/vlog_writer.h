//
// Created by 14037 on 2022/5/25.
//

#ifndef LEVELDB_VLOG_WRITER_H
#define LEVELDB_VLOG_WRITER_H

#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "leveldb/env.h"

namespace leveldb {
    namespace vlog {
        class VlogWriter {
        public:
            explicit VlogWriter(WritableFile *dest):dest_(dest),head_(0){};
            explicit VlogWriter(WritableFile *dest, uint64_t head):dest_(dest), head_(head){};
            leveldb::Status AddRecord(uint64_t log_number, WriteBatch *src_batch, WriteBatch *meta_batch);
            Status Sync();
        private:
            WritableFile *dest_;
            uint64_t head_;

            Status Write(Slice data);
        };
    }
} // leveldb

#endif //LEVELDB_VLOG_WRITER_H
