//
// Created by 14037 on 2022/5/31.
//

#include <iostream>
#include "util/vlog_coding.h"

using namespace leveldb;
using namespace vlog;

Status CheckMetaCode(uint64_t log_number, uint64_t offset, uint64_t size){
    std::string meta;
    uint64_t rea_log_number, real_offset, real_size;
    meta = EncodeMeta(log_number, offset, size);
    Status status = DecodeMeta(meta, &rea_log_number, &real_offset, &real_size);
    if(!status.ok()) return status;
    std::string result;
    result = EncodeMeta(rea_log_number, real_offset, real_size);
    if(meta != result) return Status::Corruption("Result Not Match");
    return Status::OK();
}

Status CheckKVCode(Slice key, Slice value){
    std::string kv;

    std::string real_key, real_value;
    ValueType real_type;

    kv = EncodeKV(key, value);
    Slice kvs(kv);

    Status status = DecodeKV(&kvs, &real_key, &real_value, &real_type);
    if(!status.ok() || real_type != kTypeValue) return status;

    std::string tmp;
    tmp = EncodeKV(real_key, real_value);
    if (kv != tmp) return Status::Corruption("Result Not Match");

    return Status::OK();
}

Status CheckRecordCode(Slice data){
    Slice input;
    EncodeRecord(&input, data);

    Slice kv;
    Status status = DecodeRecord(input, &kv);
    if(!status.ok()) return status;

    Slice temp;
    EncodeRecord(&temp, kv);
    if(input.compare(temp) != 0) return Status::Corruption("Result Not Match");
    return Status::OK();
}


Status CheckKVToRecord(Slice key, Slice value){
    // 封装为kv对字符串
    std::string dst = EncodeKV(key, value);

    // 封装为带crc、长度的record
    Slice result;
    EncodeRecord(&result, Slice(dst));

    // 解析record为kv
    Slice kv;
    Status status = DecodeRecord(result, &kv);
    if(!status.ok()) return status;

    // 从kv中获得kv
    std::string get_key, get_value;
    ValueType type;

    // 这里kv是正常的，传入函数就不正常了，导致了bug的出现
    status = DecodeKV(&kv, &get_key, &get_value, &type);
    if(!status.ok()) return status;

    // 校验
    if (type == kTypeValue && get_key == key.ToString() && get_value == value.ToString()) return Status::OK();
    return Status::Corruption("Result Not Match");
}

void RunAndPrint(const char* name, const Status& status){
    fprintf(stderr, "=== Test %s: ", name);
    if(!status.ok()){
        fprintf(stderr, "[%s]\n", status.ToString().data());
        abort();
    } else{
        fprintf(stderr, "[PASS]\n");
    }
}

int main(int argc, char** argv) {
    RunAndPrint("Meta", CheckMetaCode(2123, 3434, 234234));

    RunAndPrint("KV", CheckKVCode("cxxxxxdcdczcdxxxxxxxx", "cdcsdcsdcvfvcxfdvdccd"));
    RunAndPrint("Record", CheckRecordCode("value"));
    RunAndPrint("KVToRecord", CheckKVToRecord("key", "value"));

    fprintf(stderr, "PASS\n");

    return 0;
}