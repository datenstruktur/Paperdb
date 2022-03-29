// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}
//Add()->Flush()->Finish()

void TableBuilder::Add(const Slice& key, const Slice& value) {
  // 保存了一些变量
  Rep* r = rep_;
  assert(!r->closed);

  if (!ok()) return;
  /*
   * 问: 这个if语句有什么用？
   * 答: 验证key的有序性
   *
   * 问: 为什么要判断r->num_entries > 0
   * 答: 如果DataBlock都没有KV对，就没有有序性可言了，如果有KV，新加入的Key必须比前者大
   */
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  /*
   * 问: 下面的if语句是为了实现什么功能？
   * 答: 写入index_block
   *
   * 问: index_block是干嘛的？
   * 答: 一个SSTable有很多的DataBlock，所以需要确定KV在哪一个DataBlock
   *
   * 问: DataBlock长度不是固定的吗？
   * 答: 是有有一个限度，但是有可能Add一个Key前，数据量没有达到这个限度，Add之后又刚好超过这个限度，所以DataBlock的大小还是不固定的，所以需要额外的索引
   *
   * 问: pending_index_entry什么时候会是true？
   * 答: 调用Flush()，成功持久化一个DataBlock后
   *
   * 问: 为什么？
   * 答: 因为index_block的一条记录是<key, <offset, length>>, 指向的是一条DataBlock
   *
   * 问: 我发现index_block调用的是和DataBlock一样的接口，这说明两者有相同的结构吗？
   * 答: 不是，index_block没有调用BlockBuilder::Finish()函数，所以不生成restart，只有<key, value>组成的Entry，只不过value是<offset, length>, 它被封装成handler了
   *
   * 问: pending_handle是什么含义，是怎么被赋值的？
   * 答: 它是一个DataBlock的<offset, length>, 在TableBuilder::WriteRawBlock()中被赋值，因为DataBlock持久化后才能知道它的偏移量和长度，调用Flush()就会更新pending_handle
   *
   * 问: 每次更新就意味着更新一个DataBlock，那有没有可能更新了一个DataBlock，上一个还没被写入到index_block，就被这个覆盖了？
   * 答: 不会，因为Flush()只可能在Add调用，而Flush()后pending_index_entry肯定为true，就肯定会进入这个if，就一定可以写入index_block，跑不了
   *
   * 问: FindShortestSeparator是干嘛用的？
   * 答: 找到(r->last_key, key)中最小的值赋值给r->last_key
   *
   * 问: 可否举个例子？
   * 答: 比如说("the apple", "the orange")=>"the " + 1 => ”the b“, 大概就是取两者的公共前缀然后向后推一格
   *
   * 问: 这个FindShortestSeparator有什么用？
   * 答: 你可以发现，找到的这个新key，比last_key，也就是刚刚已经持久化的DataBlock的最后一个key，key，当前DataBlock的第一个key中间，那么它就大于刚刚已经持久化的DataBlock的所有Key，小于当前DataBlock的所有key
   *
   * 问: 对，那为什么不直接把last_key或者key写入index_block？
   * 答: 你没有发现经过FindShortestSeparator处理之后产生的key更短吗？这样可以节省index_block的key的空间
   *
   * 问: 如果调用这次Add是最后一个DataBlock的最后一个key，那这个if还能不能进去？
   * 答: 不能，因为最后一个DataBlock的第一个key Add时，就已经进入这个if语句，把倒数第二个DataBlock的handle<offset, length>添加到了index block了
   *
   * 问: 那最后一个DataBlock的handle<offset, length>岂不是加不了了？毕竟持久化完成后就不再执行Add了
   * 答: 对，可以留到持久化index block之前把最后一个DataBlock的handle写入index block，那个时候最后一个DataBlock肯定已经写入了
   *
   * 问: 那没有key了，怎么寻找last_key和key之间的key？
   * 答: 那好办，就把key看作是无限大的就行，那么我们应该找的是比last_key大的最短字符串，比如说“helloworld”=>"h"+1 = "i"
   */
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 产生在刚刚持久化的DataBlock最后一个key与当前DataBlock第一个key之间的key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // 把持久化的DataBlock的offset和length组合成handle转换为可变32位int到handle_encoding
    r->pending_handle.EncodeTo(&handle_encoding);
    // 把<key, handle>写入index_block，由于index_block没有restart pointer，所以不执行BlockBuilder::finish()
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    // 刚刚持久化的DataBlock的index_block 信息已经写入了，关闭这个if语句的门口，等下一个DataBlock持久化后重新设置为true
    r->pending_index_entry = false;
  }

  /*
   * 问: 下面的if语句实现了什么功能？
   * 答: 计算key的hash，写入filter data
   *
   * 问: 如何实现的？
   * 答: 直接调用AddKey，key就会被写入缓冲区等待计算hash值
   *
   * 问: key什么时候被计算hash，生成filter data？
   * 答: 等一个新DataBlock被持久化后，如果新的数据满2^(baselg)Bytes之后，就会计算hash，生成filter data
   */
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  /*
   * 问: 下面的代码块实现了什么功能？
   * 答: 更新一些东西，包括last_key为当前key、Entry（DataBlock的KV对）数量、向DataBlock加入一个Entry（KV对）
   *
   * 问: last_key不是一个DataBlock的最后一个key吗？怎么每Add一个key就设置为last_key?
   * 答: last_key只会在上面 if (r->pending_index_entry)这个代码块中使用，进入到这个代码块的前提就是一个DataBlock刚满，那么此时的key，也就是last_key就是DataBlock的最后一个key
   */
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  /*
   * 问: 下面的代码实现了什么功能？
   * 答: 如果此key加入后DataBlock刚好满了，就可以持久化这个DataBlock了
   *
   * 问: 为什么有CurrentSizeEstimate？
   * 答: 因为Add只把Key-Value加入了缓冲区，restart数据没有加入缓冲区，所以只能预估大小来判断预估DataBlock是否满了
   *
   * 问: DataBlock满了之后调用的Flush()是什么？
   * 答: 是持久化DataBlock的函数
   */
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // DataBlock不能为空，否则和没有东西可以flush了
  if (r->data_block.empty()) return;

  // pending_index_entry必须为false，要求上一个DataBlock的<offset, length>更新入了index_block
  assert(!r->pending_index_entry);
  // 调用Flush说明Add进内存的DataBlock数据已经满了，所以需要持久化r->data_block，持久化后把<offset, length>赋值到r->pending_handle
  WriteBlock(&r->data_block, &r->pending_handle);
  // 写入成功，没有出错
  if (ok()) {
    // 则重新把pending_index_entry设置为true，让在下次调用Add()后刚生成的pending_handle就可以被更新入index_block
    r->pending_index_entry = true;
    // 调用磁盘IO持久化buffer里的数据到磁盘，但是实际上数据还是可能在文件系统甚至是FTL的buffer里，但是对于存储引擎来说，已经持久化了
    r->status = r->file->Flush();
  }
  // 持久化成功一个DataBlock，那么就需要给这些数据生成Filter Data
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

/*
 * 问: 为什么要设计这个函数？
 * 答: TableBuilder::Flush()需要把DataBlock持久化，然后把DataBlock的<offset, length>赋值给handle，供TableBuiler::Add()里来加入index_block
 *
 * 问: 它还有什么功能？
 * 答: 支持数据压缩之后再写入
 *
 * 问: 所有调用该函数进行写入的过程，都是要进行数据压缩的吗？
 * 答: 是的，因为是否要压缩由配置参数r->options.compression决定，它默认是采用snappy压缩的
 *
 * 问: leveldb里所有block的写入都是压缩的吗？
 * 答: 不是，你只要不调用WriteBlock而调用WriteRawBlock就行
 *
 * 问: 它可以用来持久化哪些block？
 */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;

  // 调用Finish给DataBlock加上restart信息，使之完整
  Slice raw = block->Finish();

  Slice block_contents;
  // 读取是否要压缩的配置参数，默认是采用Snappy压缩
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  // 如果需要压缩，则调用函数压缩，如果不需要，就保持不变
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 写入压缩/未压缩的数据和type，把持久化后的DataBlock的handle<offset, length>赋值给handle
  WriteRawBlock(block_contents, type, handle);

  r->compressed_output.clear();
  // 清除DataBlock builder的内部使用的内存块，以便下次Finish()
  block->Reset();
}

/*
 * 问: 为什么要设计一个函数？
 * 答: WriteBlock需要写入压缩/未压缩的数据、type，把持久化后DataBlock的handle<offset, length>赋值给handle变量
 *
 * 问: 如何把持久化后DataBlock的handle<offset, length>赋值给handle变量？
 * 答: length好解决，直接把传入的block_contents的size取出来就行，而当前要持久化的DataBlock就是上一个持久化DataBlock的尾地址，所以只要把上一次DataBlock的尾地址赋值给offset就可以了
 */
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // 当前的DataBlock文件的尾地址作为将要持久化的DataBlock的首地址，它就是handle里的offset
  handle->set_offset(r->offset);
  // 把要持久化的数据（可能经过了压缩）的大小作为handle里的length
  handle->set_size(block_contents.size());
  // 把DataBlock写入文件
  r->status = r->file->Append(block_contents);

  // 给DataBlock加上type和作crc校验
  if (r->status.ok()) {
    // kBlockTrailerSize = 5, type长度是1byte，crc是32bit（4bytes），所以总共是5bytes
    char trailer[kBlockTrailerSize];
    trailer[0] = type; // 把type加入到第一个byte
    // 压缩成crc
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    // 把刚加入的type也进行crc，并合并到原来的crc中
    /*
     * 问: 为什么不先把type加入到block_contents中，然后直接进行crc压缩计算，而是先计算DataBlock，再计算crc，再加入type，计算crc，合并呢？
     * 答: @doubt
     */
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type

    // 把32bit的crc转换为char写入trailer，当然不能覆盖加入的type，所以需要跳过trailer数组的第一个元素
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));

    // 写入type后crc校验值
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));

    // 如果成功，就更新DataBlock文件的尾地址，作为下一个要持久化的DataBlockd的首地址
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 给filter data写上filter offset信息然后持久化它，把获得的handle赋值给filter_block_handle
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  /*
   * 问: 为什么要有metaindex_block？
   * 答: 因为DataBlock的大小是不确定的，所以我们并不知道某一个SSTable的DataBlock在什么地方结束，Filter Block从哪里开始，所以需要有一块地方保持它开始的偏移量
   *
   * 问: 为什么metaindex_block还需要有key，像restart pointer一样有offset不就行了吗？
   * 答: 因为leveldb打算在将来支持多种过滤器，可能会有多个filter block
   */
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name()); //从配置文件中获得过滤器的名字，现在是布隆过滤器

      //加入value，meta index block的value是filter block的<offset, length>，封装为handle
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    // 把meta index_block进行持久化后把产生的handle<offset, length>赋值到metaindex_block_handle
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  /*
   * 问: index block里有什么？
   * 答: <maxkey, handle<offset, length>>，maxkey是比当前DataBlock所有key都大，比下一个DataBlock的key都小的key
   *
   * 问: 在持久化index block之前为什么还需要做一次这样的操作？
   * 答: 因为之前添加DataBlock的handle到index block，是在Add下一个DataBlock的第一个key的过程中完成的，这样的话最后一个DataBlock没法添加handle，所以只能在要持久化index block前添加了
   *
   * 问: 这个时候最后一个DataBlock的handle已经产生了吗？
   * 答: 那肯定的，DataBlock是第一批持久化的。
   */
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }

    // 持久化index_block并把产生的handle赋值到index_block_handle里
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  /*
   * 问: footer是干嘛用的？
   * 答: 是用来指示index block和meta index block的，index block是用来指示DataBlock的，meta index block是用来指示filter block
   *
   *
   */
  if (ok()) {
    // 创建footer，添加metaindex_block_handle和index_block_handle
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);

    // 编码成可变32字节int以节省空间
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);

    // 持久化入文件
    r->status = r->file->Append(footer_encoding);

    // 更新文件偏移量
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
