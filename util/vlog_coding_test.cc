/*
 * vlog各种编解码函数的单元测试
 * vlog_coding.cc
 * 在clion中选择vlog_coding_test运行就可以执行
 */

#include <iostream>
#include "util/vlog_coding.h"

using namespace leveldb;
using namespace vlog;

// 检查kv的编解码功能
Status CheckKVCode(Slice key, Slice value){
    // 把key-value编码为字符串
    std::string kv = EncodeKV(key, value);
    Slice kvs(kv);

    // 把字符串解码回key-value
    std::string real_key, real_value;
    ValueType real_type;
    Status status = DecodeKV(&kvs, &real_key, &real_value, &real_type);
    if(!status.ok() || real_type != kTypeValue) return status;

    // 把key-value再编码为字符串
    std::string tmp;
    tmp = EncodeKV(real_key, real_value);

    // 经过编码-解码-再编码，得到的值与原来的值不相同，就报错
    if (kv != tmp) return Status::Corruption("Result Not Match");
    return Status::OK();
}

// 检查record的编解码功能
Status CheckRecordCode(Slice data){
    // 把data编码为record，把数据存到input
    Slice input;
    EncodeRecord(&input, data);

    // 把保存有record数据的input解码为数据到kv
    Slice kv;
    Status status = DecodeRecord(input, &kv);
    if(!status.ok()) return status;

    // 把数据kv再编码为record数据到temp
    Slice temp;
    EncodeRecord(&temp, kv);

    // 如果把解码到的数据再编码，和刚开始编码data的数据不一样的话，就是报错了
    if(input.compare(temp) != 0) return Status::Corruption("Result Not Match");
    return Status::OK();
}

// 检查从kv对到可以在磁盘中保存的record的整个流程
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

// 检查meta数据的编解码
Status CheckMetaCode(uint64_t log_number, uint64_t offset, uint64_t size){
    // 把三者编码为字符串meta
    std::string meta = EncodeMeta(log_number, offset, size);

    // 从字符串meta中解码三者
    uint64_t rea_log_number, real_offset, real_size;
    Status status = DecodeMeta(meta, &rea_log_number, &real_offset, &real_size);
    if(!status.ok()) return status;

    // 再次把三者编码为字符串
    std::string result;
    result = EncodeMeta(rea_log_number, real_offset, real_size);

    // 如果把解码出来的三者编码成的字符串和直接编码的字符串不一样，就说明有问题
    if(meta != result) return Status::Corruption("Result Not Match");

    return Status::OK();
}

// 每次检查完都会有一个status数据
// 成功的话status.ok为真
// 失败的话status.ok为假，并保存有错误信息
void RunAndPrint(const char* name, const Status& status){
    fprintf(stderr, "=== Test %s: ", name);
    if(!status.ok()){
        fprintf(stderr, "[%s]\n", status.ToString().data());
        abort();
    } else{
        fprintf(stderr, "[PASS]\n");
    }
}

/*
 * 结果:
 * === Test Meta: [PASS]
 * === Test KV: [PASS]
 * === Test Record: [PASS]
 * === Test KVToRecord: [Corruption: type not match]
 */
int main(int argc, char** argv) {
    RunAndPrint("Meta", CheckMetaCode(2123, 3434, 234234));
    RunAndPrint("KV", CheckKVCode("cxxxxxdcdczcdxxxxxxxx", "cdcsdcsdcvfvcxfdvdccd"));
    RunAndPrint("Record", CheckRecordCode("value"));
    // 这里出了问题
    RunAndPrint("KVToRecord", CheckKVToRecord("key", "value"));

    fprintf(stderr, "PASS\n");
    return 0;
}