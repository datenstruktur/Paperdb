//
// Created by 14037 on 2022/6/10.
//

#include <iostream>
#include "vlog_gc.h"
#include "db/vlog_reader.h"
#include "util/vlog_coding.h"
#include "filename.h"

namespace leveldb {
    uint64_t vlog::VlogGC::GCVlog(const std::string& dbname, uint64_t log_number) {
        auto *reader = new vlog::VlogReader(dbname, log_number);
        Slice record;
        std::string scratch;

        SequenceNumber sn;
        std::string key, value, get_value;
        ValueType type;
        Env *env = Env::Default();
        uint64_t head, tail = 0;
        env->GetFileSize(LogFileName(dbname, log_number), &head);
        uint64_t left = 0;

        while (reader->ReadRecord(&record, &scratch)){
            DecodeKV(record.ToString(), &sn, &key, &value, &type);
            if(type == kTypeValue){
                // 如果可以查询到，且查询的值准确
                mux_->Unlock();
                if(db_->Get(ReadOptions(), key, &get_value).ok() && get_value == value){
                    mux_->Unlock();
                    // 重新放入db中
                    db_->Put(WriteOptions(), key, value);
                    left += record.size();
                }
            }
            tail += record.size();
            if(tail >= head) break;
        }

        return (head - left);
    }

    void vlog::VlogGC::CleanFile(){
        Env *env = Env::Default();
        for(const auto& i : vlog_){
            env->RemoveFile(LogFileName(i.first, i.second));
        }
    }

    void vlog::VlogGC::StartGC() {
        uint64_t gc = 0;
        for(const auto& i : vlog_){
            gc += GCVlog(i.first, i.second);
        }
        CleanFile();
        vlog_.clear();
    }

    uint64_t vlog::VlogGC::GetFileSize(const std::string &dbname, uint64_t log_number) {
        Env *env = Env::Default();
        uint64_t filesize;
        Status status = env->GetFileSize(LogFileName(dbname, log_number), &filesize);
        if(!status.ok()){
            std::cout << status.ToString() << std::endl;
            return 0;
        }
        return filesize;
    }
} // leveldb