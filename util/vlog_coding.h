//
// Created by 14037 on 2022/5/31.
//

#ifndef LEVELDB_VLOG_CODING_H
#define LEVELDB_VLOG_CODING_H

#include "leveldb/status.h"
#include "db/dbformat.h"

namespace leveldb {
    namespace vlog{
        const int kVHeaderMaxSize = 4 + 8;
        Status DecodeKV(Slice* src, std::string * key, std::string * value, ValueType *type);
        Status DecodeRecord(Slice src, Slice *kv);

        std::string EncodeKV(Slice key, Slice value);
        void EncodeRecord(Slice *result, Slice data);

        Status DecodeMeta(const std::string& addr, uint64_t *log_number, uint64_t* offset, uint64_t *size);
        std::string EncodeMeta(uint64_t log_number, uint64_t offset, uint64_t size);
    }
} // leveldb

#endif //LEVELDB_VLOG_CODING_H
