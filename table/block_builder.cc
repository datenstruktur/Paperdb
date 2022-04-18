// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"

#include "util/coding.h"

namespace leveldb {

/*
 * 构造造函数，相当于初始化
 * 1. 传入options, 用到了保存的block_restart_interval和用户自定义的comparator
 * 2. 相当于执行reset
 * 3. 检查restart内entry的数量是否大于等于1
 */
BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1); //检查block_restart_interval，也就是restart内entry的数量必须大于等于1，不然怎么写入entry？
  restarts_.push_back(0);  // First restart point is at offset 0 第一个restarts_从0开始
}

/*
 * 写完一次datablock之后需要写下一次，就需要清空各种变量
 * 在TableBuilder::WriteBlock的末尾调用，也就是持久化完datablock之后重新清空
 * 1.
 */
void BlockBuilder::Reset() {
  //重置保存数据的buffer_
  buffer_.clear();

  //清空restart_pointer，由于下一此push_back之后就是下一个restart_pointer的首地址了，所以必须先push_back一个0
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0

  //restarts内的entry个数清零
  counter_ = 0;
  //因为调用了Finish，finished被置为了true，导致不可以add了，所以需要重新可以add
  finished_ = false;

  //清空保存上一个key的变量
  last_key_.clear();
}

/*
 * 预估datablock的大小，包括Entry、restart_pointer、restart_pointer_length的长度
 * 在table_builder中的Add函数中被调用，如果发现当前datablock达到阈值了，就调用TableBuilder::Flush
 * TableBuilder::Flush调用了WriteBlock持久化DataBlock
 * 在调用的WriteBlock中，真正持久化DataBlock前，就调用BlockBuilder::Finish写入了restarts_数据了
 * 所以本函数算是对写入后datablock大小的预估
 *
 * 总而言之，它只在Add数据到内存，且还没有写入restarts前被调用
 */
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

/*
 * 向buffer_写入restart_pointer和restart_pointer_length
 * 在table_builder的WriteBlock函数里被调用，持久化前才需要写入restart_pointer
 *
 * 1. 写入restarts_数组内的restart_pointer
 * 2. 写入restarts_数组的长度也就是restart_pointer_length
 * 3. 设置标志位让Add不能写入Entry以防止数据写入乱套
 * 4. 把buffer_返回
 */
Slice BlockBuilder::Finish() {
  // Append restart array
  /*
   * 写入restarts，每个元素都是每个restarts开始的offset
   * 问：key呢？没有key这么进行二分查找？
   * @highlight 答：不用存储key，到时候二分查找的时候直接用下标取到offset，再根据offset从Entry里取到完整的key就可以了，这样可以节省空间
   */
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }

  // 写入restart_pointer_length，读取的时候可以一下子读取所有的restart_pointer数组
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true; //已经写入restart_pointer了，就不能继续写入Entry了，不然就乱套了，Add和Flush分开，你就不能保证调用的时候是先调用完Add再调用Finish
  return Slice(buffer_); //返回buffer_
}

/*
 * 向一个DataBlock的buffer_(尚未持久化)写入一个键值对，当前key与前一个key共享公共前缀，每隔block_restart_interval个KV对保存一个完整的key
 * 问：为什么这样可以压缩数据？压缩效果好吗？
 * @highlight 答：因为SSTable是只读的，Add接口这可能在两种情况下被调用，
 *      一是从memtable的skiplist中dump下来，
 *      二是Major Compaction的时候合并生成的的有序序列写入
 *    这两种情况key都是有序写入的，有序就意味着前后两个key相似度是最高的，所以可以如此压缩，压缩的效果应当很好
 *
 * 问：为什么不采用同样的手段压缩value呢？
 * 答：因为value是无序的，前后两个value的公共前缀短，压缩后提升不多，而压缩/解压缩是需要浪费CPU资源的
 *
 * 问：为什么不一组的key和第一个key共享公共前缀？
 * 答：可能是这样的公共前缀太短了，甚至没有，所以没意义
 *
 * 问：为什么要间隔block_restart_interval共享公共前缀
 * 答：因为读取到一个key片段后，是需要把本restart内所有的key都读出来才能恢复key的，所以restart不能太长
 *
 * 问：怎么似曾相识？
 * 答：你可能说的是Trie树（Prefix Tree）
 *
 * 问：怎么实现的
 * 答：1.先是进行一些合法性检查，比如说是否调用了finish停止了Add、Entry的个数是否超过了上限、插入的key是否有序
 *    2.计算出本次插入的key与上次key的公共前缀长度
 *    3.把key的长度（共享的公共前缀长度、非共享的长度）、value的长度写入到buffer_
 *    4.把key_delta、value写入buffer_
 *    5.把当前的key作为last_key_
 */
void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_); //把上一此插入的key转为为slice类型

  assert(!finished_); // 如果插入开始前调用了Finish()，则不能继续Add了

  assert(counter_ <= options_->block_restart_interval); // 当前restart的Entry的数量不能比最大限度大

  /*
   * @highlight 检查插入数据的有序性，插入的key必须是递增的
   *  1.如果buffer_是空的，说明没有插入过数据，就不需要做有序性检查
   *  2.如果不为空，就需要做有序性检查，本次插入的key必须比上一个插入的key大，不然无法插入
   */
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);

  /*
   * 计算本次加入的Key需要被共享的长度
   * 要求：
   *    1. 如果未产生一个新restart，则计算出与前一个key的公共前缀的长度shared
   *    2. 如果产生一个新的restart，则当前key不与任何key共享数据，shared == 0
   * 过程：
   *    1. 确定与当前key共享的last_key_piece
   *        a. 如果当前Entry没有超过最大值，则说明last_key_piece不变
   *        b. 否则需要把当前key作为last_key_piece
   *    2. 计算key非共享的长度non_shared
   */
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    /*
     * 求出要共享的最大长度
     * last_key_piece.size() > key.size(),整个key都是共享的，共享的最大长度是key.size()
     * last_key_piece.size() <= key.size(), 最大共享的长度是last_key_piece.size()
     * 综上所述，共享的最大长度是两者的最小值
     */
    const size_t min_length = std::min(last_key_piece.size(), key.size());

    /*
     * 计算两个key的相似长度
     * 1. 共享长度从0慢慢增加到最大共享长度
     * 2. 如果发现当前位置的字符不一样，说明两者相似的地方就终止了
     * 3. 否则相似长度加一
     */
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else { //当前共享Key的Entry已满，当前Entry作为新的开始，key作为接下来key的公共key
    // Restart compression
    //把当前的以保存的Entry的末尾地址（就是要加入的新Entry的头地址）作为新restart的offset
    /*
     * 问: 为什么要把一组的末地址作为它的restart point的offset，而不是第一个Entry的首地址？
     * 答: 因为ParseNextKey()就是根据上一次Parse的Entry的value_地址来确定下一个Entry的首地址的，所以为了兼容它，找一组的第一个Entry的首地址时，需要找到上一组的最后一个Entry的value_的末地址
     */
    restarts_.push_back(buffer_.size());
    counter_ = 0; //清空当前restart所对应的Entry数量，重新开始
    // 本来还需要把当前key作为last_key_的
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  /*
   * 本质上是写入Key和Value的长度
   * 1. Entry里有Key和Value，因为Key和Value是首尾衔接的
   * 2. 所以必须同时保存它们的长度key_length以及value_length
   * 3. 我们需要压缩key，保存在Entry的key可能是只是一个片段
   * 4. 这个片段是与其它Key不共享的，所以长度称之为non_shared，名称称之为key_delta
   * 5. 还有一部分是与其它Key共享的，所以长度称之为shared
   *
   * 问：Varint32是什么？
   * 答：是可变的int，最大长度是32bit，由1-4个8bit单元构成，每个单元有1bit标识是否有下一个单元，可以看作一个链表
   * 问：为什么使用它存储长度？
   * 答：因为长度是可变的，有些时候会特别小，使用固定长度的32bit的int保存小长度会浪费空间，varint的长度是可变的，可以根据长度的长度变化而改变长度
   * 问：为什么先把所有的长度保存在一起，后保存kv的值？
   * 答：@noidea
   */
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  /*
   * 本质上是写入Key和Value的数据
   * 问：如何计算key_delta?
   * 答：只要确定key_delta的头和尾指针即可
   *    [key_header....key_delta_header....key_tail]
   *    key_header与key_delta_header之间的数据是共享的，长度是shared，所以key_delta_header = key_header + shared
   *    key_delta_header与key_tail之间的数据是key分别存储的，长度是non_shared，所以key_tail = key_delta_header + non_shared
   *    因为key_delta是[key_delta_header, key_tail]
   *    就是[key_header + shared, key_delta_header + non_shared]
   *
   * 问：要是是新产生一个restart，则append的是完整的key吗？
   * 答：是，因为此时shared = 0, non_shared = key.size()
   */
  buffer_.append(key.data() + shared, non_shared); //写入key_delta
  buffer_.append(value.data(), value.size()); //写入value

  // Update state

  /*
   * 把last_key_更新为当前key的值
   * 问：为什么不直接给last_key_赋值为key？
   * @highlight leveldb的实现少拷贝了share的部分，提升了性能
   *
   *    1. 舍弃上一个key不与当前key共享的部分
   *    2. 加上当前key的特殊的部分，key_delta
   */

  last_key_.resize(shared); //舍弃非共享的部分
  last_key_.append(key.data() + shared, non_shared); //加上key_delta
  assert(Slice(last_key_) == key); // 保证正确

  /*
   * 1. 没有新产生一个新restart：当前restart新加入了一个Entry
   * 2. 有产生一个新restart，之前counter_就已经清零了，此时加1是新加入的第一个完整的Entry
   */
  counter_++;
}

}  // namespace leveldb
