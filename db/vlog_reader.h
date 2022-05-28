//
// Created by 14037 on 2022/5/25.
//

#ifndef LEVELDB_VLOG_READER_H
#define LEVELDB_VLOG_READER_H

#include <atomic>
#include "leveldb/status.h"
#include "leveldb/env.h"
#include "dbformat.h"

namespace leveldb {
    namespace vlog {
        class VlogReader {
            public:
                explicit VlogReader(std::string dbname, uint64_t log_number);
                Status Read(uint64_t offset, uint64_t size,Slice *value);
                static Status DecodeKV(Slice src, std::string * key, std::string * value, ValueType *type);
                static Status DecodeRecord(Slice src, Slice *kv);
            private:
                SequentialFile *file_;
                bool JumpTo(uint64_t offset);
        };
    }
} // leveldb

#endif //LEVELDB_VLOG_READER_H
