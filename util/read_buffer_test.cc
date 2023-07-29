//
// Created by 14037 on 2023/7/29.
//
#include "util/read_buffer.h"
#include "gtest/gtest.h"

namespace leveldb {
char* AlignedMalloc(){
#ifdef _WIN32
  //TODO make 1024 become a variable
  return reinterpret_cast<char *>(_aligned_malloc(1024, 1024));
#else
  char *buf = nullptr;
  if(posix_memalign(reinterpret_cast<void **>(&buf), 1024,
                     1024) != 0){
    return nullptr;
  }
  return buf;
#endif
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
  ASSERT_NE(ptr, nullptr);
  read_buffer.SetPtr(ptr, /*aligned=*/true);
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
  ASSERT_NE(ptr1, nullptr);
  read_buffer.SetPtr(ptr1, /*aligned=*/true);

  char* ptr2 = AlignedMalloc();
  ASSERT_NE(ptr2, nullptr);
  read_buffer.SetPtr(ptr2, /*aligned=*/true);
}
}