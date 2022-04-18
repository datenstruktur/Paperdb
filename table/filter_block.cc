// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// AddKey->GenerateFilter->Finish->StartBlock

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

/*
 * 问: 为什么要设计这个函数？
 * 答: 因为当一个DataBlock被持久化后，对于布隆过滤器来说就来了新活，它需要为这些新的DataBlock数据生成新的filter data，所以就需要这个函数
 *
 * 问: 一个DataBlock生成后是不是可能生成多个filter data？
 * 答: 是可能的，因为布隆过滤器是每隔固定长度的DataBlock数据生成一个新的filter data，而这个间隔是可以调节的，如果这个间隔相对于一个DataBlock很小，就有可能生成多个filter data
 *
 * 问: 那生成的filter block的个数如何计算呢？
 * 答: 我们知道已经有的filter data的个数filter_offsets_.size()，那么我们只需要知道添加了新数据后，需要多少个filter data，我们就知道还需要新产生多少个filter data了
 *
 * 问: 那么我们如何知道添加了新数据后，需要多少个filter data呢？
 * 答: 把添加完DataBlock后DataBlock数组数据的长度整除间隔就知道了，这个值可以被叫做filter_index
 *
 * 问: 整除是向下取整啊，那剩下的不足间隔长度的数据怎么办？要生成filter data吗？
 * 答: 当然不要，你这要是不足间隔长度就生成一个filter data，到时候查找的时候就找不到了，留给下一次筹齐间隔长度的数据再生成filter data，所以实际需要存在的filter data的个数就是filter_index
 *
 * 问: 所以整除的结果一定要比已经有的filter data个数大呗？这才说明产生了大于间隔的DataBlock数据，需要生成一个新的filter block
 * 答: 对，上面说了调用filter_offsets_.size()就可以知道现在已经生成了多少个filter data，因为每产生一个Filter Data就会产生一个对应的filter_offset_[]数组元素
 *
 * 问: ！！！我总算知道filter_offset为什么不保存在一个完整的内存块中而是保存在数组了！！这样可以很方便地通过size函数拿到filter data的个数
 * 答: 对！而且更加奇妙的是，每加入一个fiter data，filter_offset_.size()都会自己加一噢！，就不需要写额外的代码了
 *
 * 问: 我们要求持久化新的DataBlock之后需要的filter data的个数filter_index大于filter_offset_.size()，size()又是自己自增的，那么我们可以构建一个while循环，条件是前面说的，执行语句是产生一个filter data？
 * 答: 对，本函数就是这么设计的
 */
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  //
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

/*
 * 问: 为什么要设计这个函数？
 * 答: 在把一个Key-Value加入到DataBlock的同时，还需要计算它的hash值加入到bitmap中
 *
 * 问: 它在什么时候被调用？
 * 答: TableBuilder::Add()中，向DataBlock Add完Key-Value之后就Add key到bitmap
 *
 * 问: 从底层来说，key是被进行了hash运算了之后直接添加到了bitmap里了吗？
 * 答: @doubt 不是，Key只是被暂时存在一个内存里，因为你不知道本层的bitmap有多长，需要凑齐所有的key
 *
 * 问: key被以这样的形式存储在内存中呢？
 * 答: 以append only的形式保存在key_s中
 *
 * 问: 这样的话如何区分相邻的key呢？
 * 答: AddKey的时候还保存了每个Key相当于start_的首地址，两个首地址隔开一个key
 */
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size()); //未AddKey前，保存key_的尾地址就是要Add的Key的相对keys_开头的首地址
  keys_.append(k.data(), k.size()); //写入k, keys_.size()会更新，向前推进k.size()
}

/*
 * 问: 为什么要设计这个函数？
 * 答: 你先问如何查到一个key在哪一个Filter Data。
 *
 * 问: 如何查到一个key在哪一个Filter Data？
 * 答: 首先Filter Data中是没有key的，所以单单有Filter Data是查询不到的。而我们知道，一个Filter Data表示的是一定长度的DataBlock的key，
 * 所以，只要知道key在所有DataBlock组成的整体里的offset，实际上就知道了key的hash值存在哪个Filter Data。
 *
 * 问: 但是我看bloom.cc里的CreateFilter，产生的Filter Data的大小是随着key的数量的变化而变化的，而每生成一次Filter Data是按照长度来的，key的数量不一定每次都一样，而Filter Data又是首尾紧密排列在一切的，
 * 即使知道是第几个Filter Data，也确定不了具体是哪一个啊！
 * 答: 所以在所有Filter Data后面加上它们的filter_offset，这些filter_offset按顺序保存着每一个Filter Data的地址，由于知道了是第几个Filter Data，filter_offset又是按顺序排列的，所以就可以找到Filter
 * Data对应的哪个filter offset，就可以定位到那个Filter Data上了
 *
 * 问: 我明白了，所以为什么要设计这个函数？
 * 答: 因为要在Filter Data后面加上这些Filter Data的filter_offset
 *
 * 问: 如何添加？
 * 答: filter_offset产生于GenerateFilter，保存在filter_offsets_[]这个数组里，只需要按顺序遍历每一个元素然后写入result_就可以了
 *
 * 问: 每个Filter Data大小都不固定，那么Filter Data的整体的大小也不固定，把Filter Data和filter offset放在一起，我怎么知道filter offset从哪一块地方开始？
 * 答: 确实不知道，但是写入result_的之前知道，filter offset的开头就是Filter Data的结尾，所以可以把结尾的偏移量filter offset's offset写到filter offset后面，就行了
 *
 * 问: 读的时候怎么读？
 * 答: 读的时候反过来读就可以先读到它，确定filter offset的数据，然后找到当前Filter Data的首地址和下一个Filter Data的首地址，把它们包围的数据块读取出来就行。
 *
 * 问: 前面提到，leveldb的设计是每隔固定长度的DataBlock生成一个Filter Data，那么这个长度可以调整吗？
 * 答: 可以，这个变量叫Base lg，表示每隔2^(Base lg)Byte的DataBlock生成一个Filter Data，默认是11，也就是每隔2KB的DataBlock数据生成一个Filter Data
 *
 * 问: 那我怎么知道当前这个filter block是每隔多少数据生成一个Filter Data？
 * 答：Base lg是保存在filter block里的，由于读取时需要第一个读，所以写入时候需要最后一个写入，所以写完filter data的首地址偏移量filter offset's offset之后，还需要写入Base lg
 *
 * 问：实际上这个函数需要向原先保存Filter Data的result_中再写入每个Filter Data对应的filter offset，再写入filter offset的首地址filter offset' offset，以及最后的base lg？
 * 答: 对，最后以Slice的形式返回，因为这个函数没有持久化的功能，需要交给其它函数来持久化
 *
 */
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

/*
 * 问: 为什么要设计这个函数？
 * 答: 为了在必要的时候把AddKey输入到key_的全部key生成一个Filter Data，也就是布隆过滤器的bitmap，同时保存一个Filter Data对应的filter offset
 *     它们在磁盘中大概的格式是
 *     Filter Data1
 *     Filter Data2
 *     ......
 *     Filter Datan
 *     Filter Offset1
 *     Filter Offset2
 *     ......
 *     Filter Datan
 *
 * 问: 这个函数是要把数据组织成上面的格式吗？
 * 答: 不是，它只是生成一个Filter Data1到一块叫result_的内存中，在把filter offset保存在一个数组中，之后再合成到一个大内存中
 *
 * 问: 它在什么时候被调用？
 * 答: FilterBlockBuilder::StartBlock,
 * 生成DataBlock的时机为预测到它的长度可能会超过上限，
 * 而Filter Data是产生与DataBlock之上的，所以Filter Data的产生由持久化DataBlock来触发
 *
 * 问: key从哪里来？
 * 答: 从string变量key_来，AddKey时，把key追加到了它后面
 *    key_的大概格式是 key1, key2, key3...keyn
 *
 * 问: key首尾衔接排列到key_数组中，怎么区分相邻的key？
 * 答: AddKey的时候还把Key相对key_首地址的偏移量加入到了start_，两个首地址偏移量隔出一个Key
 *    start_的大概格式是 [key1_header, key2_header,...,key_headern]
 *
 * 问: 要更新的filter_offset是什么？
 * 答: 是本次要更新的Filter Data的首地址
 *
 * 问: filter_offset怎么求？
 * 答:  1. 本条Filter Data的首地址就是上一条Filter Data的尾地址，
 *     2. 而Filter Data保存在result_里
 *     3. 最后一条Filter Data就是上一条
 *     4. 所以要添加的Filter Data的首地址就是result_的尾地址，就是result_.size()
 *
 * 问: bitmap如何生成？
 * 答: 读取key_中各个key，通过布隆过滤器生成bitmap
 *
 * 问: 如何从key_中读出一个Key？
 * 答: 1. 计算key的base(一个key的绝对首指针)
 *     2. 计算出key的length
 *     3. 然后需要从key_中把这些key读出来，保存在一个临时变量里
 *
 * 问: 如何计算base，一个key的绝对首指针？
 * 答: key_的绝对首指针 + 这个key在key_中的相对首地址偏移量
 *
 * 问: 如何如何计算一个key的长度？
 * 答: 把它下一个key的相对key_的首地址偏移量减去它自己的相对首地址偏移量
 *
 * 问: 最后一个key没有下一个key怎么办？
 * 答: 提前向start_中保存它下一个key的偏移量，它就是key_的末地址，下一个key添加到这里
 *
 * 问: 生成的bitmap放在哪儿？
 * 答: 追加在变量result_里
 */
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size(); //获得key的个数
  if (num_keys == 0) {
    /*
     * 问: 为什么没有key还需要更新filter_offsets_?
     * 答: @todo
     */
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation，这样方便计算最后一个key的长度
  tmp_keys_.resize(num_keys); //重新调整tmp_keys_以保存num_keys个key, 可以看作一个可变数组

  // 准备好key，从key_中读取每一个key到tmp_keys_中
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i]; //取第i个key
    size_t length = start_[i + 1] - start_[i]; //取第i个key的长度
    tmp_keys_[i] = Slice(base, length); //把第i个key存到tmp_keys_中
  }

  // Generate filter for current set of keys and append to result_.
  /*
   * 问: 为什么要把filter_offset存在一个数组中而不是和filter data一样存在内存块中？
   * 答: @highlight 看FilterBlockBuilder::StartBlock里的注释
   */
  filter_offsets_.push_back(result_.size()); //写入filter offset
  // 从tmp_keys_开始向后数num_keys个key，生成一个Filter Data到result_里
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  /*
   * 问: 为什么要销毁这些保存key的变量？
   * 答: 因为这些变量实际上只保存一个Filter Data的数据，本次Filter Data已经生成了，所以就没有用了，需要清空等待生成下一个Filter Data使用
   *
   * 问: 为什么不销毁保存Filter Data的result_呢？
   * 答: 因为只生成了一个Filter Data，整个Filter Block有好多个Filter Data，所以尚未生成完，不能清除
   *
   * 问: 为什么BlockBuilder.cc中生成一个DataBlock就设计了一个独立的reset函数来清除保存DataBlock数据的buffer_呢？
   * 答: @highlight 因为一个SSTable有多个DataBlock，生成完一个下一个DataBlock还需要用到buffer_， 但是只有一个FilterBlock，所以没必要清除result_，下一个SSTable会直接创建自己的buffer_
   */
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

/*
 * 过滤器读取的初始化
 * contents: filter data block
 */
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1]; //最后一个bit是base lg
        uint32_t last_word = DecodeFixed32(contents.data() + n - 5); //filter offset's offset + base_lg_
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word; //定位到filter 1 offset
  num_ = (n - 5 - last_word) / 4; //
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
