// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

// 把iter里的kv对构建成一个sstable持久化到dbname开头的文件里，并生成一个FileMetaData
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  // 初始化文件大小
  meta->file_size = 0;
  // 移动到kv序列的第一个kv
  iter->SeekToFirst();

  // 创建sstable的文件名
  std::string fname = TableFileName(dbname, meta->number);

  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    // 第一个key就是最小的key
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key(); //读取kv序列的一个key
      // 加入到table中
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      // 最后一个key是最大的key
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish(); //完成sstable的构建
    if (s.ok()) {
      // 取得sstable的文件大小
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      // 持久化sstable到磁盘
      s = file->Sync();
    }
    if (s.ok()) {
      // 关闭文件
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      // 加载到lrucache中
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else { //如果构建失败，就删除文件
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
