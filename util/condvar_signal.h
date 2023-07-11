//
// Created by WangTingZheng on 2023/7/11.
//

#ifndef LEVELDB_CONDVAR_SIGNAL_H
#define LEVELDB_CONDVAR_SIGNAL_H

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {
class SCOPED_LOCKABLE CondVarSignal {
 public:
  explicit CondVarSignal(port::Mutex* mu, bool *done,
                         port::CondVar* condVar) EXCLUSIVE_LOCK_FUNCTION(mu)
      :mu_(mu), done_(done), condVar_(condVar){
    *this->done_ = false;
    this->mu_->Lock();
  }

  CondVarSignal(const CondVarSignal& ) = delete;
  CondVarSignal& operator=(const CondVarSignal&) = delete;

  ~CondVarSignal()UNLOCK_FUNCTION(){
    *this->done_ = true;
    this->mu_->Unlock();
    this->condVar_->SignalAll();
  }

 private:
  port::Mutex* const mu_;
  port::CondVar* const condVar_;
  bool *const done_;
};
}
#endif  // LEVELDB_CONDVAR_SIGNAL_H
