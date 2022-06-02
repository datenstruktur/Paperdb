/*
 * 本文件是基于google test编写的vlog读写功能的单元测试，测试的文件有:
 * db/vlog_reader.cc
 * db/vlog_reader.h
 * db/vlog_writer.cc
 * db/vlog_writer.h
 * 测试的过程主要分为以下几步：
 * 1. 初始化（VlogTest()）: 主要是初始化writebatch和writer，reader用到的时候初始化
 * 2. 准备数据（Put/Delete）: 向src_batch中写入一条数据
 * 3. 写入数据（SyncBatch）: 把装满数据的sr_batch写入磁盘文件并把key-kv的地址写入临时的temp_batch
 * 4. 读取数据
 *      a. 随机读取: 根据保存有key-kv地址的write batch从文件中读取kv对到dst_batch
 *      b. 顺序读取: 遍历磁盘文件，读取kv对到iter_batch
 * 5. 检查读取准确性
 *      a. 随机读取:
*/

#include <iostream>
#include "leveldb/env.h"
#include "db/filename.h"
#include "db/vlog_writer.h"
#include "db/vlog_reader.h"
#include "db/write_batch_internal.h"
#include "gtest/gtest.h"
#include "util/crc32c.h"
#include "util/vlog_coding.h"

namespace leveldb {
    using namespace vlog;
    class VlogTest : public testing::Test {
        public:
            VlogTest(uint64_t log_number, const std::string &dbname)
                    : log_number_(log_number),
                      dbname_(dbname),
                      env_(Env::Default()),
                      src_batch_(new WriteBatch()),
                      syn_batch_(new WriteBatch()),
                      reader_batch_(new WriteBatch()),
                      iter_batch_(new WriteBatch()),
                      meta_batch_(new WriteBatch()),
                      reader_(nullptr) {
                WritableFile *dest;
                env_->NewWritableFile(LogFileName(dbname, log_number), &dest);
                writer_ = new vlog::VlogWriter(dest);
            }

            VlogTest(): log_number_(1),
                        dbname_("dbname"),
                        env_(Env::Default()),
                        src_batch_(new WriteBatch()),
                        syn_batch_(new WriteBatch()),
                        reader_batch_(new WriteBatch()),
                        iter_batch_(new WriteBatch()),
                        meta_batch_(new WriteBatch()),
                        reader_(nullptr) {
                WritableFile *dest;
                env_->NewWritableFile(LogFileName("dbname", 1), &dest);
                writer_ = new vlog::VlogWriter(dest);
            }

            // 向src batch追加一条put记录
            void Put(const Slice &key, const Slice &value) {
                src_batch_->Put(key, value);
            }

            // 向src batch追加一条delete记录
            void Delete(const Slice &key) {
                src_batch_->Delete(key);
            }

            // 把src batch的数据写入磁盘，得到key-meta<log number, offset, size>写入tmp batch
            Status SyncBatch() {
                assert(src_batch_ != nullptr);
                assert(writer_ != nullptr);

                // 把src batch中的put数据写入到磁盘，delete的留下
                // put数据产生的key-meta写入tmp batch
                Status status = writer_->AddRecord(log_number_, src_batch_, syn_batch_);
                if (!status.ok()) return status;

                // 刷新数据
                status = writer_->Sync();
                if (!status.ok()) return status;

                return Status::OK();
            }

            // tmp batch的key对应的meta，从磁盘中读取出key-value，写入dst batch
            // 把tmp batch的delete的key直接加入dst batch
            Status ReadBatch() {
                assert(reader_batch_ != nullptr);
                reader_batch_->Clear();

                uint64_t pos = 0;
                SequenceNumber sn = WriteBatchInternal::Sequence(syn_batch_);
                Slice key, meta, record;
                ValueType type;

                Status status;
                std::string kv;

                SequenceNumber real_sn;
                std::string real_key, real_value;
                ValueType real_type;

                uint64_t log_number, offset, size;

                // 读取tmp batch中的key-meta
                while (syn_batch_->ParseBatch(&pos, &key, &meta, &type).ok()) {
                    if (type == leveldb::kTypeValue) { // 如果是put的，需要解析meta读取真正的key-meta
                        // 解析meta为log number、offset、size
                        status = DecodeMeta(meta.ToString(), &log_number, &offset, &size);
                        if (!status.ok()) return status;

                        // 根据meta数据初始化reader，读取key-value的数据到record
                        reader_ = new vlog::VlogReader(dbname_, log_number);
                        status = reader_->Read(offset, size, &record);
                        if (!status.ok()) return status;

                        // 解析record数据为kv数据
                        status = DecodeRecord(record.ToString(), &kv); //进行crc校验，提取出kv
                        if (!status.ok()) return status;

                        // 从kv数据中解析出key-value
                        status = DecodeKV(kv, &real_sn, &real_key, &real_value, &real_type);
                        if (!status.ok()) return status;

                        // 检查tmp batch的key和读取的key是否一致
                        if (key.compare(real_key))
                            return Status::Corruption("read key is not match");

                        if(sn != real_sn)
                            return Status::Corruption("read sn is not match");
                        sn ++;
                        // 把key-value加入到dst batch
                        reader_batch_->Put(real_key, real_value);
                    } else if (type == leveldb::kTypeDeletion) { // 如果是删除数据，则没有value就没办法从磁盘读取数据
                        reader_batch_->Delete(key); // 直接插入dst batch
                        sn ++;
                    } else {
                        return Status::Corruption("get error type");
                    }
                }
                return Status::OK();
            }

            Status IterBatch(){
                assert(iter_batch_ != nullptr);
                iter_batch_->Clear();

                Slice record;
                std::string scratch;

                Status status;

                SequenceNumber sn;
                std::string key, value;
                ValueType type;

                reader_ = new vlog::VlogReader(dbname_, log_number_);
                reader_->Jump(0);

                while (reader_->ReadRecord(&record, &scratch)){
                    if(!record.empty()){

                    } else if(!scratch.empty()){
                        record = Slice(scratch);
                    }

                    status = DecodeKV(record.ToString(),&sn, &key, &value, &type);
                    if(!status.ok()) return status;

                    if(type == kTypeValue) {
                        iter_batch_->Put(key, value);
                    }
                }

                delete reader_;
                return Status::OK();
            }

            Status ReadMeta(){
                assert(src_batch_ != nullptr);
                assert(meta_batch_ != nullptr);
                meta_batch_->Clear();

                uint64_t pos = 0;
                SequenceNumber sn;
                std::string key, meta;
                reader_ = new vlog::VlogReader(dbname_, log_number_);
                reader_->Jump(0);

                while (reader_->ReadMeta(&pos, &key, &sn, &meta)){
                    meta_batch_->Put(key, meta);
                }

                delete reader_;
                return Status::OK();
            }

            Status CheckBatch(WriteBatch *src, WriteBatch *dst) {
                assert(src != nullptr);
                assert(dst != nullptr);

                if (WriteBatchInternal::Count(src) != WriteBatchInternal::Count(dst)) {
                    return Status::Corruption("src and dst kv count not matchs");
                }

                uint64_t src_pos = 0;
                uint64_t dst_pos = 0;

                Slice src_key, src_value;
                ValueType src_type;

                Slice dst_key, dst_value;
                ValueType dst_type;

                while ((src->ParseBatch(&src_pos, &src_key, &src_value, &src_type).ok() &&
                        dst->ParseBatch(&dst_pos, &dst_key, &dst_value, &dst_type).ok())) {
                    if((src_key != dst_key)) return Status::Corruption("key not match");

                    if (src_type == leveldb::kTypeValue) {
                        if (src_value.compare(dst_value) != 0) return Status::Corruption("value not match");
                    }
                }

                return Status::OK();
            }

            // 把src中的put的kv提取出来和dst做对比
            Status CheckPutKVBatch(WriteBatch *src, WriteBatch *dst){
                uint64_t src_pos = 0;

                Slice src_key, src_value;
                ValueType src_type;

                WriteBatch put_batch;

                while ((src->ParseBatch(&src_pos, &src_key, &src_value, &src_type).ok())) {
                    if(src_type == kTypeValue){
                        put_batch.Put(src_key, src_value);
                    }
                }

                return CheckBatch(&put_batch, dst);
            }

            Status CheckReadBatch(){
                return CheckBatch(src_batch_, reader_batch_);
            }

            Status CheckIterBatch(){
                return CheckPutKVBatch(src_batch_, iter_batch_);
            }

            Status CheckMetaBatch(){
                return CheckPutKVBatch(syn_batch_, meta_batch_);
            }

            static void PrintChar(const char *buf, int length){
                for (int i = 0; i < length; i++) {
                    printf("%c\n", buf[i]);
                }
                printf("\n");
            }

            static void PrintBatch(WriteBatch *batch){
                uint64_t pos = 0;
                Slice key, value;
                ValueType type;
                while (batch->ParseBatch(&pos, &key, &value, &type).ok()){
                    if(type == kTypeValue){
                        fprintf(stderr, "[Put    ]: key : %s, value :%s\n", key.ToString().data(), value.ToString().data());
                    } else if(type ==  kTypeDeletion){
                        fprintf(stderr, "[Delete ]: key : %s\n", key.data());
                    }
                }
            }

            static void PrintMetaBatch(WriteBatch *batch){
                uint64_t pos = 0;
                Slice key, value;
                ValueType type;

                uint64_t log_number, offset, size;
                while (batch->ParseBatch(&pos, &key, &value, &type).ok()){
                    if(type == kTypeValue) {
                        DecodeMeta(value.ToString(), &log_number, &offset, &size);
                        fprintf(stderr, "Type : %d, key : %s, log number : %lu, offset : %lu, size : %lu\n", type,
                                key.ToString().data(), log_number, offset, size);
                    }
                }
            }

            ~VlogTest() override {
                env_->RemoveFile(LogFileName(dbname_, log_number_));
                delete src_batch_;
                delete syn_batch_;
                delete reader_batch_;
                delete iter_batch_;
                delete meta_batch_;
            }

    private:
        uint64_t log_number_;
        std::string dbname_;
        vlog::VlogWriter *writer_;
        vlog::VlogReader *reader_;
        Env *env_;

        WriteBatch *src_batch_; // 用户写入的batch
        WriteBatch *syn_batch_; // 写入vlog得到的key-meta的batch，delete直接写入
        WriteBatch *reader_batch_; // 根据sync_batch读取到的key-value，没有delete的
        WriteBatch *iter_batch_; // 用readrecord遍历vlog得到的batch，没有delete的
        WriteBatch *meta_batch_; // 用readmeta遍历vlog得到的key-meta的batch
    };

    void RunAndPrint(const char* name, const Status& status){
        fprintf(stderr, "=== Test %s: ", name);
        if(!status.ok()){
            fprintf(stderr, "[%s]\n", status.ToString().data());
            abort();
        } else{
            fprintf(stderr, "[PASS]\n");
        }
    }

    TEST_F(VlogTest, TestPutAndDelete) {
        Put("cdckey1", "xdncjdsdxc cn");
        Put("keycd2", "vdsxdcalue2");
        Put("kcdcsey3", "vacdsclue3"); // value3就可以，value3qqqq就不行
        Put("keadwdey4", "vqwdqaldqque4");
        Delete("kdesdcsey5");
        Put("sdcdcsdcd", "vscbhgnaludbgbe6");
        Delete("ggthytdfghyj");
        Delete("ujsttggr");
        Put("jttreghyt", "gtdzths");
        Delete("gtshytjag");
        Put("hydhagyjy", "gsrgth");

        RunAndPrint("sync batch", SyncBatch());
        RunAndPrint("read batch", ReadBatch());
        RunAndPrint("iter batch", IterBatch());
        RunAndPrint("meta batch", ReadMeta());

        RunAndPrint("check batch", CheckReadBatch());
        RunAndPrint("check iter batch", CheckIterBatch());

        RunAndPrint("check meta batch", CheckMetaBatch());
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}