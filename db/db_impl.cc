// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

// 合并排序之后的状态
struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Compaction生成好的sstable
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)){}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

// @wisckey 新建一个DB，主要是准备好CURRENT，Manifest，相当于创建必要的文件，不然没地方写入
// 每产生一个VersionEdit都会用WAL保证持久性，VersionEdit的WAL文件就叫manifest
// 读取manifest就可以知道LevelDB的所有改动，但是如果恢复的时候恢复所有的历史，任务未免太重了
// 所以LevelDB会选择当manifest的文件太大时，创建一个新的空manifest，舍弃掉之前的VersionEdit
// 所以会有多个manifest，但是我们只需要最新的，所以我们把最新的manifest的名字保存在CURRENT上
// 这样读取CURRENT文件就可以读到manifest，就可以读到VersionEdit恢复出整个VersionSet
Status DBImpl::NewDB() {
  // 这是为了保存配置信息，所以没有SSTable的FileMateData加入
  VersionEdit new_db;

  new_db.SetComparatorName(user_comparator()->Name());// 输入比较器的名字
  // 没有VersionSet，New不出File Number，只能手动分配
  // 设置log number为0，这是第一个file number
  new_db.SetLogNumber(0);
  // manifest文件是1，所以下一个可以用的file number是2
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0); //没有kv，所以最后一个sn是0

  // 创建manifest文件的地址，1的文件编号给了manifest
  const std::string manifest = DescriptorFileName(dbname_, 1);

  // manifest文件写入的句柄
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    // 创建WAL的writer
    log::Writer log(file);

    // 把VersionEdit持久化到manifest
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record); // 持久化edit
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;

  // 把manifest文件名称写入CURRENT文件
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    // 把MANIFEST-000001的文件编号写入到current文件中
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

// @wisckey 删除一些文件，主要是用于保证memtable持久性的log文件、VersionSet持久性的manifest文件以及sstable文件
void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  // log文件：比当前VersionSet中保存的log number小的都不需要，因为它们已经被转换为sstable持久化到磁盘了
  // manifest文件：manifest太大会舍弃，同时会生成新的manifest号保存在VersionSet中，比这个小的也都不需要了
  // sstable文件：VersionSet中每一个版本中的每一层的sstable是活跃的，其它的都需要删除
  // tmp文件：这个是向CURRENT写入一行数据时产生的临时的，还没有被改为CURRENT的文件（不直接写是为了避免读写冲突）
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;// 原本保存的活跃文件
  versions_->AddLiveFiles(&live); // 获取VersionSet中所有指向的sstable的编号

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete; // 准备要删除的文件编号
  for (std::string& filename : filenames) { //遍历每一个扫描到的文件
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile: //小于当前versionset的log就没必要留下了，都持久化到sstable了
          // 可是为什么会有比versionset的log还大的log file呢？
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile: // 因为数据量大而被抛弃的manifest就删除
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) { // 如果不用保存，那么就删除
        // 加入枪毙（不是）删除名单
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) { // 如果是sstable的话还得从lrucache中删除
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) { //遍历删除名单，删除这些文件
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

// @wisckey 恢复主要分两步：
// 1)从manifest中恢复VersionSet，因为VersionSet是保存在内存中的
// 2)从WAL中恢复memtable，因为memtable也是保存在内存中的
// WAL的log number是保存在VersionEdit中的，所以必须先恢复VersionSet
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.

  // 新建数据库文件夹，所有的文件都是放在在里面的，因为有文件不存在的情况，我们就需要创建一个DB
  // 但是创建DB（也就是NewDB函数）只会朝dbname文件夹下写文件，不会创造文件夹
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);

  // 上上文件锁
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  // 如果CURRENT文件不存在，说明DB不存在，创建一个新DB完事
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) { //
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB(); // 创建一个新的数据库，需要新建一个manifest文件并把它写入CURRENT文件中
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  // 从current读取manifest并从中恢复VersionSet
  // 恢复过程中可能会复用manifest文件，如果manifest文件不是很大且可以继续写入的话
  // 不复用，save_manifest就为true
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }

  // 读取log文件中的sequence number
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  // 取得最新的log file number，可能在Minor Compaction的过程中，新的log number已经现成，文件也已经创建
  // 但是旧的log number并没有来得及加入VersionEdit
  // 或者加入VersionEdit了但是没有来得及持久化到manifest
  // 从manifest恢复成VersionSet时，读到的实际上是更旧的log number，那么实际上要恢复的log不止一个
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);

  uint64_t number;
  FileType type;
  // 读取所有的log文件
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number); //是活跃的文件
      // 更旧的log就不要了
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }

  // 如果去除扫描到文件之后活跃文件还有，那就说明有活跃的文件根本不存在，文件丢失了
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    // 恢复每一个log文件，重新赋值sequence number
    // 如果恢复造成了Minor Compaction，save_manifest就会为true
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  // 设置最后的sn，便于写入新batch的时候用
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

// @wisckey 恢复log数据到memtable，造成Minor Compaction，save_manifest为true
// last_log为true，说明是最后一个log，说明是崩溃前正在写的活跃log
// 可以选择复用这个log/产生新的log
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  // @todo 这个reporter还是需要研究
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  // 读取里面每一个batch
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    // 新建memtable
    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    // 把batch插入memtable
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }

    // 重新计算max sequence
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) { // 比原来大，就更新SN
      *max_sequence = last_seq;
    }

    // 如果mem满了
    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++; // 计算Minor Compaction的次数
      *save_manifest = true;// 记录已经进行了Minor Compaction
      // 触发Minor Compaction，直接刷新mem
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  // compactions == 0：不能进行过Minor Compaction，进行过Minor Compaction的WAL必须删除，不然数据就会重复
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) { //文件可写

      // 复用log number
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  // @todo 不复用，logfile_number怎么初始化？
  // 即使memtable，也清理掉memtable
  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

// @wisckey 传入一个待持久化的Memtable
// 返回一个VersionEdit
// Version没必要，刚开始不考虑写入不同层的情况
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  // 这个可能是benchmark用的？
  const uint64_t start_micros = env_->NowMicros();

  // 准备好SSTable的FileMetaData，并为其分配一个log number
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();

  // 添加到活跃文件列表，防止RemoveObsoleteFile()函数删除它
  pending_outputs_.insert(meta.number);

  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  // 把memtable的数据构建成一个SSTable到meta中
  Status s;
  {
    // @todo 为什么持久化的时候要放锁
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;

  // @todo 我觉得LogAndApply到VersionSet才能移除啊
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      // 传入SSTable的范围，选择添加入的层
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    // 加入VersionEdit
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);//stats是打印用的？
  return s;
}

// @wisckey Minor Compaction
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  // 传入base是为了选层用
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // @todo 这里可能会崩溃
  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0); // 听说是原来兼容老版本的
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    // @todo 是干嘛用的，为什么不直接置false
    has_imm_.store(false, std::memory_order_release);

    //@todo 这个函数是干嘛的
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

// @wisckey 
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
    // 实际的Compaction完成了才会重新变回false
    // 不能重复调用MaybeScheduleCompaction
    // 调用一次就设置为了true，直到调用BackgroundCall才会设置为false
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    // DB退出了
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
    // 有报错
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
    // 没有新生成的immetable
    // 没有触发manual compaction
    // 不需要再进行compaction
  } else {
    // 已经实际调用了，不到BackgroundCall不能重新调用
    background_compaction_scheduled_ = true;
    // 用一个后台线程调用BGWork函数
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

// 调用BackgroundCall函数
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

// @wisckey Compaction线程的真正的函数调用
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    // 实际的Compaction函数
    BackgroundCompaction();
  }

  // 一次Compaction结束，可以进行下一回Compaction
  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();

  // 跳出Compaction循环
  // 不用再Compaction了，唤醒因为Write Stall挂起的写入线程继续写入
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  // Minor Compaction
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) { // Manual Compaction
    ManualCompaction* m = manual_compaction_;
    // 根据[begin, end]获取到应该Compaction的两层SSTable到inputs_[0]和inputs_[1]
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      // inputs_[0][inputs_[0].size() - 1]
      // Compaction的上一层的最后一个SSTable的最大key
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else { //Seek Compaction
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {

    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}


Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  // 根据file_number创建sstable的文件名
  std::string fname = TableFileName(dbname_, file_number);
  // 初始化WriterFile
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    // 创建TableBuilder
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}


Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    // 构建sstable
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  //读取sstable的文件大小
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    // 持久化outfile
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) { // 如果没有snapshots
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // 为Compaction构建Iterator
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;

  // 解析key出来的变量
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  // 遍历Compaction中的数据
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      // 如果immetable还有数据
      if (imm_ != nullptr) {
        // 则先进行Minor Compaction
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key(); //获取到key
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    // 如果key和上一个重复了，则丢弃
    bool drop = false;
    // 1.解析本次key成功
    // 2.之前就有key且本轮key与上一次key相等
    // last_sequence_for_key是上一个key的sequence
    if (!ParseInternalKey(key, &ikey)) { // 把字符串key解析成结构体，如果失败
      // Do not hide error keys
      // 如果key既不是kTypeDeletion的也不是kTypeValue的
      current_user_key.clear(); //清除user_key缓存
      has_current_user_key = false; //没有user_key
      last_sequence_for_key = kMaxSequenceNumber;//key的sequence到最大
    } else {
      // key不重复，需要保留
      if (!has_current_user_key || //没有user_key
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) { //或者当前user_key与本次遍历的key不一样
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size()); //拷贝本次遍历的key到当前key
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // kv的sn比smallest_snapshot
      // 说明不在snapshot内
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion && // 如果是删除的kv
                 ikey.sequence <= compact->smallest_snapshot && //sn比snapshot的smallest sn还小，说明不在snapshot范围内
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) { //key在level - n + 1层不存在
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    // 如果不舍弃，就加入到sstable中
    if (!drop) {
      // Open output file if necessary
      // 准备好输出的sstable
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }

      if (compact->builder->NumEntries() == 0) { //如果是第一个key
        // 因为kv对是有序的，所以用它持久化smallest
        compact->current_output()->smallest.DecodeFrom(key);
      }
      // 输入sstable的key是当前最大的key
      // 下一个输入key会覆盖上一个
      compact->current_output()->largest.DecodeFrom(key);
      // 把input的kv对写入sstable
      compact->builder->Add(key, input->value());

      // 如果sstable满了
      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        // 持久化sstable
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    // 移动到下一个kv
    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  // 添加入memtable的迭代器
  list.push_back(mem_->NewIterator());
  mem_->Ref();

  // 添加immemtable的迭代器
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }

  // 添加磁盘中的version的迭代器
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

// @wisckey Get还是很简单的，memtable查不到到immutable查，查不到到SSTable中查（通过VersionSet中查）
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();

  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

// @wisckey Write演进过程
// 1. key-value直接写入memtable
// 2. 在数据库崩溃时，1会造成数据丢失，所以key-value写入memtable的时候会先写入WAL
// 3. 每次写入key-value就会产生一次磁盘写IO，为了降低磁盘写IO，把key-value封装为WriteBatch
// 4. Put、Delete接口会把一个key-value封装为一个WriteBatch，那么会有很多WriteBatch只有少量的key-value，导致写磁盘IO次数依然很高
// 5. 把多个线程写入的WriteBatch合并成一个WriteBatch，再把这个WriteBatch写入WAL
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  // 把一个batch封装成一个Writer，供LevelDB在队列中调度
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync; //同步/异步刷新，同步的话用Sync，异步的话用Flush
  w.done = false;  // 是否已经写入WAL

  // LevelDB设计了一个队列来容纳多线程放入的“写请求“，为什么要看这个呢？因为Wisckey有类似的操作
  // 为什么不用堆呢？因为writer的写入顺序要和传入顺序一致
  // 每来一个线程调用Write，都会把writer放入队列等待被调度（因为只有一个writer是队首）
  // 没有调度完成的非队首writer会始终在这里等待着
  // Stage1: Begin
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  // End

  // 能到这一步说明两种情况：
  // 1.这个writer已经被调度且写入LSM了 或
  // 2.这个writer是队首，承担着调度其它writer的责任，相当于消费者
  // Stage2: Begin
  if (w.done) { // 如果是完成了的状态
    // 就结束写入
    return w.status;
  }
  // End


  // 能到这里的都是未被调度完成的队首writer
  // 这里front的writer需要做一些全局性的工作，所以要上锁
  // 主要包括以下几点：
  // 1.对多线程共同操作的全局资源memtable/immutable进行操作，在适当的时机清理已经满的memtable，为接下来的写入腾出空间（MakeRoomForWrite）
  // 2.扫描全局资源：writer队列，合并多个WriteBatch（BuildBatchGroup）
  // 3.为WriteBatch赋值上Sequence Number并更新Last Sequence Number（这个也是全局变量）
  // Stage3
  Status status = MakeRoomForWrite(updates == nullptr);

  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    // LevelDB并不会合并队列中的全部writer，因为有些无法合并
    // 比如说前3个都要求异步刷新，第四个要求同步刷新，那么只能合并前三个
    // last_writer是合并的最后一个writer
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // End

    // 处理全局资源的过程结束了，所以接下来会放开锁，不涉及队列、Last Sequence Number
    // @todo LevelDB的memtable的跳表支持并发写入？
    // 按照writer的sync要求选择Flush还是Sync
    // AddRecord中已经有Flush了
    // 然后再插入memtable
    // Stage4

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();

      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));

      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }

      // 写入memtable
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);

      }
      // End

      // 接下来又要对Last Sequence Number、队列进行操作了，所以要上锁
      // 要进行的主要操作是：
      // 1. 更新Last Sequence Number
      // 2. 之前只是读取了队列中一些writer的数据并写入它们，它们所在的线程还在等待，所以要把它们的状态设置为done，而且要唤醒它们，这样它们就可以运行到Stage2了
      // 3. 如果队列中还有writer，那么它们也需要调度，之后传入队列的writer都不会是队首writer，而只有队首的队列可以调度其它writer，所以我们必须主动唤醒队首队列
      // Stage5
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    //这个tm_batch_是BuildBatchGroup里的，我们先不去管他
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }


  // 遍历取出整个队列的[front, last_writer]，设置为以完成调度并唤醒它们结束写入
  while (true) {
    // 弹出队列的队首writer
    Writer* ready = writers_.front();
    writers_.pop_front();

    // 这里要考虑到队首writer的情况（也就是执行这段代码的writer），它不需要被唤醒，因为它就是唤醒其它线程的线程
    if (ready != &w) {
      ready->status = status;
      // 把它们都设置为已完成写入
      ready->done = true;
      // 重新启动它们，就会跳出Stage1的while循环进入Stage2，然后跳出写流程
      ready->cv.Signal();
    }
    // 到last_writer为止，因为并不是所以队列中的writer都会被合并，之后的之后再合并
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  // 主动唤醒新的队首writer作为消费者来合并其它writer
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front(); // 队列前头的writer
  WriteBatch* result = first->batch; // 取出batch
  assert(result != nullptr);

  // 计算batch的大小
  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) { //遍历队列中的每一个writer
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// @wisckey 写之前memtable可能已经满了，所以我们需要把memtable转换为imutable，并生成一个新的空memtable为写提供空间
// 当写入的write batch为空时，force为true，那么就意味着不管memtable此时的数据量有没有达到阈值，都要生成imutable
// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  // 需要换memtable，这是一个全局性的工作，需要获得锁，如果没有获得锁就执行了这个函数，就会报错，相当于一个锁断言
  mutex_.AssertHeld();
  assert(!writers_.empty());

  // 这个变量是决定是否在出现Write Stall的时候暂时拒绝用户写入数据的
  // 如果用户都要强制刷新memtable到磁盘了，那么就没必要管Write Stall
  bool allow_delay = !force;

  // 分几种情况处理
  Status s;
  while (true) {
    if (!bg_error_.ok()) { //有问题当然得停止，得去报告
      // Yield previous error
      s = bg_error_;
      break;
      // 如果允许延迟以及第0层的SSTable的个数大于等于8，发生了Write Stall，但是不严重，所以我们决定休眠写入线程1ms，等待Major Compaction清理L0的SSTable
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      // 延时一次就够了，如果allow_delay不重新设置为false，再次写入时就会一直到这个else if来
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force && // 如果当memtable还没满不能生成immetable、memtable的实际空间没有超过阈值时
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      // 没必要处理，memtable有数据，直接跳出，用户还得写入数据呢
      break;
    } else if (imm_ != nullptr) { // 如果有immetable产生，说明需要进行Minor Compaction
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait(); // 挂起写入线程，需要先Compaction后才能写入
      // 如果L0的SSTable太多超过12，产生了严重的Write Stall，则直接停止写入而不是延时
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait(); // 挂起写入线程，需要先处理好L0层的SSTable再写入
    } else { //正常情况，也就是一个memtable满了的情况
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0); // 这个不用管，兼容旧版本的LevelDB

      // 会生成一个新的memtable，需要为它配一个WAL文件，所以需要申请一个log number
      uint64_t new_log_number = versions_->NewFileNumber();

      // 创建一个写句柄，本质上是调用文件IO的open函数
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);

      // 如果创建文件失败
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        // file number可以理解为一个时间戳，永远递增，不可能递减
        // 回收这个编号，如果在这个期间没人调用NewFileNumber获取新的文件编号的话，放回去就行
        // 但是如果有人获取了的话就不行，比如说获取的log number是10，
        // 然后有人获取了新的文件编号11，那么你如果把10放进去，下次别人获得的新的文件编号就是10，更后的文件编号反而更小
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      // memtable满了，生成了immetable了，要持久化了，原来的WAL文件就不需要了
      // 删除原来的log和logfile_
      delete log_;
      delete logfile_;

      // 要产生新的memtable，所以为其生成新的log相关的变量
      logfile_ = lfile; // 赋值新的WAL的文件
      logfile_number_ = new_log_number; // 赋值新的WAL文件号码
      log_ = new log::Writer(lfile); //为WAL创建新的log

      // 把memtable转换为immetable
      imm_ = mem_; //原memtable已经满了，转换为immetable
      // 不知道这个has_imm_是干嘛的，但是和我们的复现无关
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_); // 生成一个新memtable供用户写入
      mem_->Ref();

      // 不然就跳不出了
      force = false;  // Do not force another compaction if have room

      // 在写线程中尝试启动Compaction线程，Compaction线程是一个单独的线程
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
// @wisckey 把写入LSM的KV对封装为Batch供Write调度
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

// @wisckey LSM Tree中删除也是写入，只不过写入的是一个标志位
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

// @wisckey 和NewDB()不同，Open需要对DB进行恢复，这个是主要的工作
Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  // 新建一个数据库，但是此时状态还是空的，还没恢复
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();

  // 恢复WAL的时候会把数据重新放回memtable，所以可能会引发Minor Compaction，那么就会产生SSTable，就会修改磁盘中的状态，就会生成一个VersionEdit
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  // 这个是用来标识有没有进行Minor Compaction，也就是有没有产生VersionEdit的
  bool save_manifest = false;

  // 恢复VersionSet和Memtable
  // 恢复Memtable的时候可能会引发Minor Compaction，那么Edit会有意义，且save_manifest为true
  Status s = impl->Recover(&edit, &save_manifest);

  // 正常的恢复是要清理完memtable的数据的，但是有可能存在未创建一个空的memtable供用户写入
  // @todo 我不知道为什么，但是这里影响不大
  // 注意mem_ == nullptr和memtable是空的是有区别的，memtable为空，但是还是可以写入的
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.

    // 那就要生成一个空memtable并为它产生一个新的WAL文件了
    uint64_t new_log_number = impl->versions_->NewFileNumber(); //新建一个log number
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);

    if (s.ok()) {
      edit.SetLogNumber(new_log_number); // 设置新的number
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);

      // WAL文件已经生成好了，产生空的memtable
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }

  // 如果在恢复的过程中进行了Minor Compaction，生成了VersionEdit，那就需要应用到恢复的VersionSet上并持久化到manifest文件上
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    // 问：这个和上面的if中的第1行有什么区别？
    // 答：这个if不在上面的if内，可能memtable不为空，所以edit.SetLogNumber(new_log_number);不会被执行
    edit.SetLogNumber(impl->logfile_number_);

    // 应用到VersionSet并持久化到manifest文件中
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }

  // 因为最新的log number/VersionSet发生了变化（LevelDB不会保存所有的VersionSet），所以需要清理一些log、sstable文件
  // 开启Compaction线程，这样就回到了一个正常状态下的DB
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();

  // 返回impl（DB的操作接口，有Put/Delete/Write之类的接口）供用户调用
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
