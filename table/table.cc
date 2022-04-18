// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;

  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  // 从footer读取index_handle读取到index_block_contents
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents); // 读取index_block

    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;

    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

// 从footer读取filter_handle_value
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) { //没有过滤器就不需要读取meta block
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name()); //加上过滤器的名字
  iter->Seek(key); //查询布隆过滤器的meta index block
  if (iter->Valid() && iter->key() == Slice(key)) { // 如果查到布隆过滤器了
    ReadFilter(iter->value()); // 读取出filter block的偏移量和长度
  }
  delete iter;
  delete meta;
}

// 根据filter_handle_value从磁盘中读取block contents，把block contents解析成filterBlock
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  // 从filter_handle_value中解析出BlockHanle，这样就可以直接读取offset和length了
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block; // 局部对象
  // 根据filter_handle把filter data读取到block中
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }

  // @todo 单独保存filter block，好像没啥用？？
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  // 根据读取的filter block新建一个布隆过滤器对象
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
  // 此时执行Block的析构函数，block.heap_allocated == true，则删除block.data
}


// 到目前为止，index block和filter block都已经读出来了

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 为一个Block创建一个迭代器
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache; // 该Block对应的LRUCache
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input); // 解析block的BlockHandle
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) { // 该Block已经被读到了LRUCache
      //key: [cache_id][handle.offset]
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));

      // 在LRUCache中查询Block具体保存的单元
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) { // 如果找到了
        // 直接从LRUCache加载数据到block
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else { // 如果没找到
        // 把数据从磁盘中读取到contents中
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents); //使用contents新建一个block
          if (contents.cachable && options.fill_cache) { // 满足缓存条件
            // 向LRUCache插入这个block并返回cache handle以用来在LRUCache中取得数据
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else { // 如果此Block没有被读取到LRUCache
      // 从文件读取到数据到contents
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        // 从contents中构建block
        block = new Block(contents);
      }
    }
  }

  // block取得了，接下来需要为block设置一个迭代器
  Iterator* iter;
  if (block != nullptr) { // 如果block存在
    // 直接调用block.cc来创建迭代器
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) { //如果没有缓存在LRUCache
      iter->RegisterCleanup(&DeleteBlock, block, nullptr); //删除内存中的block
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle); //在block_cache中这个LRUCache中释放block
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

/*
 * 问: 为什么要设计这个函数？
 * 答: 为了当SSTable被读取到内存（LRUCache）后可以查找到一个kv
 *
 * 问: 查询到后怎么处理呢？
 * 答: 由传入的一个回调函数handle_result处理，查询到一对key-value时，会把key-value传入这个函数处理
 *
 * 问: 如何从一个SSTable中查询到一对kv？
 * 答: 先根据index block找datablock，再根据datablock找entry
 *     a. 先读取到index block: 在index block中进行二分查找定位到一条大于k的最小key，然后根据offset找到对应的Datablock
 *     b. 再读取datablock: 在restart pointer上进行二分查找定位到
 */
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k); //在index block中定位
  if (iiter->Valid()) {
    Slice handle_value = iiter->value(); //取出DataBlock的offset
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) { //根据datablock的offset找到filter block然后查找是否存在这个key
      // Not found
    } else { //如果过滤器指示存在
      Iterator* block_iter = BlockReader(this, options, iiter->value()); //用index block产生的offset定位一个datablock
      block_iter->Seek(k); //定位到key
      if (block_iter->Valid()) {
        //放入回调函数处理
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
