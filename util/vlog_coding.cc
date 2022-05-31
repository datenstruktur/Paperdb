//
// Created by 14037 on 2022/5/31.
//
/*
 * kv: 对key-value对的封装，特指Put的key-value对，Delete的不封装，因为不会被写入vlog
 *     Type[1Byte]-key_length[Varint32]-key-value_length[Varint32]-value
 * Record: 对写入磁盘的数据的封装
 *      CRC[Fixed32]-length[Fixed64]-Data
 * Meta: 一条Record在磁盘中的位置
 *      log_number[Fixed64]-offset[Fixed64]-size[Fixed64]
 */

#include "vlog_coding.h"
#include "crc32c.h"

namespace leveldb {
    namespace vlog{

        // 编码key/value对
        std::string EncodeKV(Slice key, Slice value) {
            std::string dst;
            // 写入type，持久化到磁盘的只有kTypeValue，也就是Put类型的数据
            dst.push_back(kTypeValue);
            // 写入key的长度和key
            PutLengthPrefixedSlice(&dst, key);
            // 写入value的长度和value
            PutLengthPrefixedSlice(&dst, value);

            return dst;
        }

        // 从src中解码key/value对到key, value, type
        Status DecodeKV(Slice* src, std::string* key, std::string* value, ValueType *type) {
            // 1是1Byte的ValueType，4是key/value的长度
            if(src->size() < 1 + 4) return Status::Corruption("src is too short!");

            // 读取第一个字符为type
            // 这里经常出问题，src到这里就变成了乱码了，type就无法准确解析
            *type = (ValueType) src->data()[0];
            if (*type != kTypeValue) return Status::Corruption("type not match");
            src->remove_prefix(1); //去除这个type的1Byte

            // 读取key/vale
            Slice tmp_key, tmp_value;
            if(!(GetLengthPrefixedSlice(src, &tmp_key) &&GetLengthPrefixedSlice(src, &tmp_value))){
                return Status::Corruption("key or value did not parse well");
            } else{
                // 赋值给key/value形参
                *key = tmp_key.ToString();
                *value = tmp_value.ToString();
                return Status::OK();
            }
        }

        // 编码数据为record
        void EncodeRecord(Slice *result, Slice data){
            const char* ptr = data.data();
            size_t left = data.size();
            // kVHeaderMaxSize = 4 + 8
            // 4为crc的长度，8为length的长度
            char buf[kVHeaderMaxSize + left];

            // 放入4Byte的crc
            uint32_t crc = crc32c::Extend(0, ptr, left);
            crc = crc32c::Mask(crc);    // Adjust for storage
            // 编码进buf
            EncodeFixed32(buf, crc);
            // 编码进buf，从第5个字符开始
            EncodeFixed64(&buf[4], left);

            // 把数据拷贝到buf的第12个char字符之后
            memcpy(&buf[kVHeaderMaxSize], ptr, left);

            // 封装为slice，不用的string的主要原因是string会截断\0
            // slice是内存中一段数据的封装(data_指向开头, data_ + size_指向末尾)
            Slice r(buf, kVHeaderMaxSize + left);

            // 赋值给Slice形参
            *result = r;
        }

        // 把保存在src中的record解码成kv
        Status DecodeRecord(Slice src, Slice *kv) {
            // 4 + 8是数据头的长度
            if(src.size() < 4 + 8) return Status::Corruption("src is too short");

            // 解析出保存的crc值
            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(src.data()));
            src.remove_prefix(4);

            // 解析出数据的长度
            uint64_t length = DecodeFixed64(src.data());
            src.remove_prefix(8);

            // 计算出数据的crc值
            uint32_t actual_crc = crc32c::Value(src.data(), length);

            //如果crc校验通过
            if(actual_crc == expected_crc){
                // 赋值kv
                *kv = Slice(src.data(), length);
                return Status::OK();
            }
            else return Status::Corruption("crc check failed");
        }

        // 把log_number、offset、size封装为meta
        std::string EncodeMeta(uint64_t log_number, uint64_t offset, uint64_t size) {
            std::string dst;
            // 封装log number
            PutFixed64(&dst, log_number);
            // 封装 offset
            PutFixed64(&dst, offset);
            // 封装 size
            PutFixed64(&dst, size);

            return dst;
        }

        // 从meta中解析出log_number, offset, size
        Status DecodeMeta(const std::string& addr, uint64_t *log_number, uint64_t *offset, uint64_t *size) {
            // meta的长度是固定的，三个Fixed64
            if(addr.size() < 24) return Status::NotFound("addr is too short");

            // 封装为slice，好去除已经处理的数据
            Slice input(addr);

            // 解析出log number
            *log_number = DecodeFixed64(input.data());
            input.remove_prefix(8);

            // 解析吃offset
            *offset = DecodeFixed64(input.data());
            input.remove_prefix(8);

            // 解析出size
            *size = DecodeFixed64(input.data());

            return Status::OK();
        }
    }
} // leveldb