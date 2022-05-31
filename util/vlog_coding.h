//
// Created by 14037 on 2022/5/31.
//

#ifndef LEVELDB_VLOG_CODING_H
#define LEVELDB_VLOG_CODING_H

#include "leveldb/status.h"
#include "db/dbformat.h"

namespace leveldb {
    namespace vlog{
        // 把key，value编码为字符串返回
        std::string EncodeKV(Slice key, Slice value);
        // 从编码成slice解码出key、value、type
        Status DecodeKV(Slice* src, std::string * key, std::string * value, ValueType *type);

        // record的头数据长度，4Byte的crc值，8Byte的length
        const int kVHeaderMaxSize = 4 + 8;
        // 把要持久化的数据data编码为record到result
        void EncodeRecord(Slice *result, Slice data);
        // 从磁盘中读取到的record数据src中解析出data数据kv
        Status DecodeRecord(Slice src, Slice *kv);

        // 把log_number、offset、size编码为字符串返回
        std::string EncodeMeta(uint64_t log_number, uint64_t offset, uint64_t size);
        // 从字符串addr中解码出log_number、offset、size
        Status DecodeMeta(const std::string& addr, uint64_t *log_number, uint64_t* offset, uint64_t *size);
    }
} // leveldb

#endif //LEVELDB_VLOG_CODING_H
