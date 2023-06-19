//
// Created by WangTingZheng on 2023/6/19.
//

#include "file_impl.h"

namespace leveldb {
  FileImpl::FileImpl() : write_offset_(0) {
    sink_ = new StringSink();
    source_ = nullptr;
  }

  void FileImpl::WriteRawFilters(std::vector<std::string> filters,
                                 BlockHandle* handle) {
    assert(!filters.empty());
    handle->set_offset(write_offset_);
    handle->set_size(filters[0].size());

    Status status;

    for (int i = 0; i < filters.size(); i++) {
      std::string filter = filters[i];
      Slice filter_slice = Slice(filter);
      status = sink_->Append(filter_slice);
      if (status.ok()) {
        char trailer[kBlockTrailerSize];
        trailer[0] = kNoCompression;
        uint32_t crc = crc32c::Value(filter_slice.data(), filter_slice.size());
        crc =
            crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        status = sink_->Append(Slice(trailer, kBlockTrailerSize));
        if (status.ok()) {
          write_offset_ += filter_slice.size() + kBlockTrailerSize;
        }
      }
    }
  }

   StringSource* FileImpl::GetSource() {
    if (source_ == nullptr) {
      source_ = new StringSource(sink_->contents());
    }
    return source_;
  }

  FileImpl::~FileImpl() {
    delete sink_;
    delete source_;
  }
};