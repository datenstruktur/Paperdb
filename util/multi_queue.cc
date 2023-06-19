#include "leveldb/multi_queue.h"

#include <unordered_map>

namespace leveldb {
MultiQueue::~MultiQueue() {}
struct QueueHandle{
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



class SingleQueue{
 public:
  SingleQueue(){
    lru_.next = &lru_;
    lru_.prev = &lru_;
  }

  ~SingleQueue(){
    for(QueueHandle* e = lru_.next; e != nullptr && e != &lru_;){
      QueueHandle* next = e->next;
      FreeHandle(e);
      e = next;
    }
  }

  QueueHandle* Insert(const Slice& key, FilterBlockReader* reader,
                      void (*deleter)(const Slice&, FilterBlockReader*)){
    QueueHandle* e = // -1 for first char in key
        reinterpret_cast<QueueHandle*>(malloc(sizeof(QueueHandle) - 1 + key.size()));
    e->reader = reader;
    e->deleter = deleter;
    e->key_length = key.size();
    std::memcpy(e->key_data, key.data(), key.size());

    Queue_Append(&lru_, e);

    return e;
  }

  void Erase(QueueHandle* handle){
    Queue_Remove(handle);
    FreeHandle(handle);
  }
 private:
  QueueHandle lru_;

  void Queue_Append(QueueHandle *list, QueueHandle* e){
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

  void FreeHandle(QueueHandle* e){
    if(e){
      e->deleter(e->key(), e->reader);
      free(e);
    }
  }
};

class InterMultiQueue: public MultiQueue{
 public:
  explicit InterMultiQueue():usage_(0),last_id_(0){
    queues_.resize(filters_number + 1);
    for(int i = 0; i < filters_number + 1; i++){
      queues_[i] = new SingleQueue();
    }
  }

  ~InterMultiQueue() override {
    for(int i = 0; i < filters_number + 1; i++){
      delete queues_[i];
    }
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

  Handle* Lookup(const Slice& key) override {
    auto iter = map_.find(key.ToString());
    if(iter != map_.end()) {
      QueueHandle* handle = map_.find(key.ToString())->second;
      return reinterpret_cast<Handle*>(handle);
    }else{
      return nullptr;
    }
  }

  void Release(Handle* handle) override {

  }

  FilterBlockReader* Value(Handle* handle) override {
    if(handle) {
      return reinterpret_cast<QueueHandle*>(handle)->reader;
    }else{
      return nullptr;
    }
  }

  void Erase(const Slice& key) override {
    auto iter = map_.find(key.ToString());
    if(iter != map_.end()) {
      QueueHandle* handle = iter->second;
      map_.erase(key.ToString());
      int number = handle->reader->FilterUnitsNumber();
      SingleQueue* queue = queues_[number];
      usage_ -= handle->reader->Size();
      queue->Erase(handle);
    }
  }

  uint64_t NewId() override { return ++last_id_; }

  size_t TotalCharge() const override { return usage_; }
 private:
  size_t usage_;
  uint64_t last_id_;

  std::vector<SingleQueue*> queues_;

  std::unordered_map<std::string, QueueHandle*> map_;
};

MultiQueue* NewMultiQueue() { return new InterMultiQueue(); }
}