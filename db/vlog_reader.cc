//
// Created by 14037 on 2022/5/25.
//

#include <iostream>
#include "vlog_reader.h"
#include "db/filename.h"
#include "vlog_writer.h"
#include "util/crc32c.h"
#include "util/vlog_coding.h"

namespace leveldb {
    namespace vlog{

        VlogReader::VlogReader(std::string dbname, uint64_t log_number) :checksum_(true), backing_store_(new char[kBlockSize]), buffer_(), eof_(
                false){
            std::string path = LogFileName(dbname, log_number);
            Env *env = Env::Default();
            env->NewSequentialFile(path, &file_);
        }

        Status VlogReader::Read(uint64_t offset, uint64_t size, Slice *value) {
            if(!JumpTo(offset)){
                return Status::Corruption("Jump to offset failed");
            }

            char *buf = new char[size];
            Status status = file_->Read(size, value, buf);
            if (!status.ok()) return status;
            return Status::OK();
        }

        bool VlogReader::JumpTo(uint64_t offset) {
            if (offset > 0) {  //跳到距file文件头偏移pos的地方
                Status skip_status = file_->Jump(offset);
                if (!skip_status.ok()) {
                    return false;
                }
            }
            return true;
        }

        bool VlogReader::ReadRecord(Slice* record, std::string* scratch) {
            scratch->clear();
            record->clear();

            if (buffer_.size() < kVHeaderMaxSize) {  //遇到buffer_剩的空间不够解析头部时
                if (!eof_) {
                    size_t left_head_size = buffer_.size();
                    if (left_head_size > 0)  //如果读缓冲还剩内容，拷贝到读缓冲区头
                        memcpy(backing_store_, buffer_.data(), left_head_size);
                    buffer_.clear();
                    Status status = file_->Read(kBlockSize - left_head_size, &buffer_,
                                                backing_store_ + left_head_size);

                    buffer_ = Slice(backing_store_, buffer_.size() + left_head_size);

                    if (!status.ok()) {
                        buffer_.clear();
                        eof_ = true;
                        return false;
                    } else if (buffer_.size() < kBlockSize) {
                        eof_ = true;
                        if (buffer_.size() < kVHeaderMaxSize) return false;
                    }
                } else {
                    if (buffer_.size() < kVHeaderMaxSize) {
                        buffer_.clear();
                        return false;
                    }
                }
            }
            //解析头部
            uint64_t length = 0;
            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(
                    buffer_.data()));  //早一点解析出crc,因为后面可能buffer_.data内容会变
            buffer_.remove_prefix(4);
            length = DecodeFixed64(buffer_.data());
            buffer_.remove_prefix(8);
            if (length <= buffer_.size()) {
                //逻辑记录完整的在buffer中(在block中)
                if (checksum_) {
                    uint32_t actual_crc = crc32c::Value(buffer_.data(), length);
                    if (actual_crc != expected_crc) {
                        return false;
                    }
                }
                *record = Slice(buffer_.data(), length);
                buffer_.remove_prefix(length);
                return true;
            } else {
                if (eof_) {
                    return false;  //日志最后一条记录不完整的情况，直接忽略
                }
                //逻辑记录不能在buffer中全部容纳，需要将读取结果写入到scratch
                scratch->reserve(length);
                size_t buffer_size = buffer_.size();
                scratch->assign(buffer_.data(), buffer_size);
                buffer_.clear();
                const uint64_t left_length = length - buffer_size;
                //如果剩余待读的记录超过block块的一半大小，则直接读到scratch中
                if (left_length > kBlockSize / 2) {
                    Slice buffer;
                    scratch->resize(length);
                    Status status =
                            file_->Read(left_length, &buffer,
                                        const_cast<char*>(scratch->data()) + buffer_size);
                    if (!status.ok()) {
                        return false;
                    }
                    if (buffer.size() < left_length) {
                        eof_ = true;
                        scratch->clear();
                        return false;
                    }
                } else {  //否则读一整块到buffer中
                    Status status = file_->Read(kBlockSize, &buffer_, backing_store_);

                    if (!status.ok()) {
                        return false;
                    } else if (buffer_.size() < kBlockSize) {
                        if (buffer_.size() < left_length) {
                            eof_ = true;
                            scratch->clear();
                            return false;
                        }
                        //这个判断不要也可以，加的话算是优化，提早知道到头了，省的read一次才知道
                        eof_ = true;
                    }
                    scratch->append(buffer_.data(), left_length);
                    buffer_.remove_prefix(left_length);
                }
                if (checksum_) {
                    uint32_t actual_crc = crc32c::Value(scratch->data(), length);
                    if (actual_crc != expected_crc) {
                        return false;
                    }
                }
                *record = Slice(*scratch);
                return true;
            }
        }

        Status VlogReader::Jump(uint64_t offset) {
            if(offset < 0) return Status::Corruption("offset smaller than 0");
            return file_->Jump(offset);
        }
    }
} // leveldb