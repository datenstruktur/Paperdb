//
// Created by 14037 on 2022/6/10.
//

#ifndef LEVELDB_VLOG_GC_H
#define LEVELDB_VLOG_GC_H

#include <atomic>
#include "leveldb/db.h"
#include <map>
#include "include/leveldb/env.h"
#include "filename.h"

namespace leveldb {
    namespace vlog {
        class VlogGC {
            public:
                explicit VlogGC(uint64_t threshold, DB *db, port::Mutex *mux):threshold_(threshold), db_(db), filesize_(0), mux_(mux){}

                bool AddLog(const std::string& dbname, uint64_t log_number){
                    vlog_.insert(std::make_pair(dbname, log_number));
                    uint64_t filesize = GetFileSize(dbname, log_number);
                    filesize_ += filesize;
                    fprintf(stderr, "file size %ld\n", filesize);
                    return (filesize_ >= threshold_);
                }

                bool ShouldGC(){return  (filesize_ >= threshold_);}

                void StartGC();

            private:
                uint64_t GCVlog(const std::string& dbname, uint64_t log_number);
                uint64_t GetFileSize(const std::string& dbname, uint64_t log_number);
                void CleanFile();

                uint64_t threshold_;
                DB *db_;
                port::Mutex *mux_;

                uint64_t filesize_;
                std::map<std::string, uint64_t> vlog_;
        };
    }
} // leveldb

#endif //LEVELDB_VLOG_GC_H
