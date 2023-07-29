//
// Created by 14037 on 2023/7/29.
//
#include "read_buffer.h"
namespace leveldb{

ReadBuffer::ReadBuffer():ptr_(nullptr),aligned_(false){}

void ReadBuffer::SetPtr(char* ptr, bool aligned){
  FreePtr();
  ptr_ = ptr;
  aligned_ = aligned;
}

void ReadBuffer::FreePtr(){
  if(!aligned_){
    free(ptr_);
  }else {
#ifdef _WIN32 //64-bit/32-bit Windows
    _aligned_free(ptr_);
#else
    free(ptr_);
#endif
  }

  ptr_ = nullptr;
}

ReadBuffer::~ReadBuffer(){
  FreePtr();
}
};