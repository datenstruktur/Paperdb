//
// Created by 14037 on 2023/7/29.
//
#include "util/read_buffer.h"
#include "gtest/gtest.h"

namespace leveldb {


char* AlignedMalloc(){
#ifdef _WIN32
  return reinterpret_cast<char *>(_aligned_malloc(GetPageSize(), GetPageSize()));
#else
  char *buf = nullptr;
  if(posix_memalign(reinterpret_cast<void **>(&buf), GetPageSize(),
                     GetPageSize()) != 0){
    return nullptr;
  }
  return buf;
#endif
}

TEST(ReadBufferTest, GetPageSize) {
  ASSERT_TRUE(IsAligned(GetPageSize()));
}

TEST(ReadBufferTest, Empty) {
  ReadBuffer read_buffer;
}

TEST(ReadBufferTest, Base) {
  ReadBuffer read_buffer;
  char* ptr = (char*)malloc(sizeof(char) * 10);
  read_buffer.SetPtr(ptr, /*aligned=*/false);
}

TEST(ReadBufferTest, BaseAligned) {
  ReadBuffer read_buffer;
  char* ptr = AlignedMalloc();
  read_buffer.SetPtr(ptr, /*aligned=*/false);
}

TEST(ReadBufferTest, Multi) {
  ReadBuffer read_buffer;
  char* ptr1 = (char*)malloc(sizeof(char) * 10);
  read_buffer.SetPtr(ptr1, /*aligned=*/false);

  char* ptr2 = (char*)malloc(sizeof(char) * 10);
  read_buffer.SetPtr(ptr2, /*aligned=*/false);
}

TEST(ReadBufferTest, MultiAligned) {
  ReadBuffer read_buffer;
  char* ptr1 = AlignedMalloc();
  read_buffer.SetPtr(ptr1, /*aligned=*/true);

  char* ptr2 = AlignedMalloc();
  read_buffer.SetPtr(ptr2, /*aligned=*/true);
}
}