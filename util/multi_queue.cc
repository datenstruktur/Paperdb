#include "leveldb/multi_queue.h"

#include <iostream>
#include <unordered_map>

namespace leveldb {
MultiQueue::~MultiQueue() {}
struct QueueHandle {
  FilterBlockReader* reader;
  void (*deleter)(const Slice&, FilterBlockReader* reader);
  QueueHandle* next;
  QueueHandle* prev;

  size_t key_length;
  char key_data[1];  // Beginning of key

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

void FreeHandle(QueueHandle* handle) {
  handle->deleter(handle->key(), handle->reader);
  free(handle);
}

class SingleQueue {
 public:
  SingleQueue() {
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
  }

  ~SingleQueue() {
    // all entry should be unref
    for (QueueHandle* e = in_use_.next; e != &in_use_;) {
      QueueHandle* next = e->next;
      FreeHandle(e);
      e = next;
    }
  }

  QueueHandle* Insert(const Slice& key, FilterBlockReader* reader,
                      void (*deleter)(const Slice&, FilterBlockReader*)) {
    QueueHandle* e =  // -1 for first char in key
        reinterpret_cast<QueueHandle*>(
            malloc(sizeof(QueueHandle) - 1 + key.size()));
    e->reader = reader;
    e->deleter = deleter;
    e->key_length = key.size();
    std::memcpy(e->key_data, key.data(), key.size());

    Queue_Append(&in_use_, e);

    return e;
  }

  void Erase(QueueHandle* handle) {
    if (handle != nullptr) {
      Queue_Remove(handle);
      FreeHandle(handle);
    }
  }

  void Remove(QueueHandle* handle) { Queue_Remove(handle); }

  void MoveToMRU(QueueHandle* handle) {
    Remove(handle);
    Queue_Append(&in_use_, handle);
  }

  void FindColdFilter(uint64_t& memory, SequenceNumber sn,
                      std::vector<QueueHandle*>& filters) {
    QueueHandle* e = in_use_.prev;
    do {
      if (e == nullptr || e == &in_use_) {
        break;
      }
      QueueHandle* next = e->prev;
      if (e->reader->IsCold(sn) && e->reader->CanBeEvict()) {
        memory -= e->reader->OneUnitSize();
        filters.push_back(e);
      }
      e = next;
    } while (memory > 0);
  }

 private:
  QueueHandle in_use_;  // ref >= 2 && in_cache == true

  void Queue_Append(QueueHandle* list, QueueHandle* e) {
    // Make "e" newest entry by inserting just before *list
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;
  }

  void Queue_Remove(QueueHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
  }
};

class InterMultiQueue : public MultiQueue {
 public:
  explicit InterMultiQueue() : usage_(0), last_id_(0), logger_(nullptr) {
    queues_.resize(filters_number + 1);
    for (int i = 0; i < filters_number + 1; i++) {
      queues_[i] = new SingleQueue();
    }
  }

  ~InterMultiQueue() override {
    for (int i = 0; i < filters_number + 1; i++) {
      delete queues_[i];
    }
  }

  SingleQueue* FindQueue(QueueHandle* handle) {
    FilterBlockReader* reader = Value(reinterpret_cast<Handle*>(handle));
    if (reader != nullptr) {
      int number = reader->FilterUnitsNumber();
      SingleQueue* queue = queues_[number];
      return queue;
    }
    return nullptr;
  }

  Handle* Insert(const Slice& key, FilterBlockReader* reader,
                 void (*deleter)(const Slice&, FilterBlockReader*)) override {
    assert(reader != nullptr);
    int number = reader->FilterUnitsNumber();
    SingleQueue* queue = queues_[number];
    QueueHandle* handle = queue->Insert(key, reader, deleter);
    map_.insert(std::make_pair(key.ToString(), handle));
    usage_ += reader->Size();
    return reinterpret_cast<Handle*>(handle);
  }

  std::vector<QueueHandle*> FindColdFilter(uint64_t memory, SequenceNumber sn) {
    SingleQueue* queue = nullptr;
    std::vector<QueueHandle*> filters;
    for (int i = filters_number; i > 1; i--) {
      queue = queues_[i];
      queue->FindColdFilter(memory, sn, filters);
    }

    return filters;
  }

  bool CanBeAdjusted(const std::vector<QueueHandle*>& cold, QueueHandle* hot) {
    double original_ios = 0;
    for (QueueHandle* handle : cold) {
      if (!handle->reader->CanBeEvict()) return false;
      original_ios += handle->reader->IOs();
    }
    original_ios += hot->reader->IOs();

    double adjusted_ios = 0;
    for (QueueHandle* handle : cold) {
      adjusted_ios += handle->reader->EvictIOs();
    }
    adjusted_ios += hot->reader->LoadIOs();
    adjusted_ios += (cold.size() + 1.0);

    if (adjusted_ios < original_ios) {
#if NDEBUG

#else
      if(logger_ != nullptr) {
        Log(logger_,
            "Cold BF Number: %zu, Filter Units number of Hot BF: %zu, adjusted "
            "ios: %f, original ios: %f",
            cold.size(), hot->reader->FilterUnitsNumber(), adjusted_ios,
            original_ios);
      }
#endif
      return true;
    }
    return false;
  }

  void LoadHandle(QueueHandle* handle){
    int number = handle->reader->FilterUnitsNumber();
    assert(number + 1 <= filters_number);
    queues_[number]->Remove(handle);
    queues_[number + 1]->MoveToMRU(handle);
    handle->reader->LoadFilter();
  }

  void EvictHandle(QueueHandle* handle){
    int number = handle->reader->FilterUnitsNumber();
    assert(number - 1 >= 0);
    queues_[number]->Remove(handle);
    queues_[number - 1]->MoveToMRU(handle);
    handle->reader->EvictFilter();
  }

  void ApplyAjustment(const std::vector<QueueHandle*> &colds, QueueHandle* hot){
    for(QueueHandle* cold : colds){
      EvictHandle(cold);
    }

    LoadHandle(hot);
  }

  void Adjustment(QueueHandle* hot_handle, SequenceNumber sn){
    if(hot_handle->reader->CanBeLoaded()) {
      size_t memory = hot_handle->reader->OneUnitSize();
      std::vector<QueueHandle*> cold = FindColdFilter(memory, sn);
      if (!cold.empty() && CanBeAdjusted(cold, hot_handle)) {
        ApplyAjustment(cold, hot_handle);
      }
    }
  }

  bool KeyMayMatch(Handle* handle, uint64_t block_offset, const Slice& key) override{
    FilterBlockReader* reader = Value(handle);
    QueueHandle* queue_handle = reinterpret_cast<QueueHandle*>(handle);
    FindQueue(queue_handle)->MoveToMRU(queue_handle);

    ParsedInternalKey parsedInternalKey;
    if(ParseInternalKey(key, &parsedInternalKey)) {
      SequenceNumber sn = parsedInternalKey.sequence;
      Adjustment(queue_handle, sn);
    }

    return reader->KeyMayMatch(block_offset, key);
  }

  Handle* Lookup(const Slice& key) override {
    auto iter = map_.find(key.ToString());
    if(iter != map_.end()) {
      QueueHandle* handle = map_.find(key.ToString())->second;
      return reinterpret_cast<Handle*>(handle);
    }else{
      return nullptr;
    }
  }

  FilterBlockReader* Value(Handle* handle) override {
    if(handle) {
      return reinterpret_cast<QueueHandle*>(handle)->reader;
    }else{
      return nullptr;
    }
  }

  void Erase(Handle* handle) override {
    QueueHandle* queue_handle = reinterpret_cast<QueueHandle*>(handle);
    std::string key = queue_handle->key().ToString();
    auto iter = map_.find(key);
    if(iter != map_.end()) {
      map_.erase(key);
      SingleQueue* queue = FindQueue(queue_handle);
      usage_ -= queue_handle->reader->Size();
      queue->Erase(queue_handle);
    }
  }

  uint64_t NewId() override { return ++last_id_; }

  size_t TotalCharge() const override { return usage_; }

  void SetLogger(Logger* logger) override{
    logger_ = logger;
  }
 private:
  size_t usage_;
  uint64_t last_id_;
  Logger* logger_;

  std::vector<SingleQueue*> queues_;
  std::unordered_map<std::string, QueueHandle*> map_;
};

MultiQueue* NewMultiQueue() { return new InterMultiQueue(); }
}