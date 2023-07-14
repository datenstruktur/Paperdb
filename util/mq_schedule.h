//
// Created by WangTingZheng on 2023/7/10.
//

#ifndef LEVELDB_MQ_SCHEDULE_H
#define LEVELDB_MQ_SCHEDULE_H

#include <queue>

#include "port/port.h"
namespace leveldb {

struct BackgroundWorkItem {
  explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
      : function(function), arg(arg) {}

  void (*const function)(void*);
  void* const arg;
};

class Scheduler {
 public:
  Scheduler(): background_work_cv_(&background_work_mutex_),
                 started_background_thread_(false){}
  void Schedule(void (*background_work_function)(void* background_work_arg),
    void* background_work_arg);

  void SetSignal(bool* flag, port::Mutex* mutex, port::CondVar* cv);

  void BackgroundThreadMain();
  static void BackgroundThreadEntryPoint(Scheduler* env){
    env->BackgroundThreadMain();
  }
 private:
  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  port::Mutex *main_thread_destructor_mutex_;
  bool* main_thread_destructor_wait_;
  port::CondVar* main_thread_destructor_cv_;

  void WakeUpDestructor();
  void LockDestructor();

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);
};

class MQSchedule{
 public:
  static MQSchedule* Default();
  void ScheduleLoader(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg);
  void ScheduleUser(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg);

  void SetSignal(bool *user_flag, bool *loader_flag,
                 port::Mutex* mutex, port::CondVar* cv);

 private:
  Scheduler loader;
  Scheduler user;
};

}  // namespace leveldb

#endif  // LEVELDB_MQ_SCHEDULE_H
