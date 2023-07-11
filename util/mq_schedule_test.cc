//
// Created by WangTingZheng on 2023/7/10.
//
#include "mq_schedule.h"

#include <gtest/gtest.h>

#include "leveldb/env.h"

#include "mutexlock.h"

namespace leveldb{

class MQScheduleTest : public testing::Test {
 public:
  MQScheduleTest():env_(MQSchedule::Default()){}

  MQSchedule* env_;
};

TEST_F(MQScheduleTest, RunImmediately) {
  struct RunState {
    port::Mutex mu;
    port::CondVar cvar{&mu};
    bool called = false;

    static void Run(void* arg) {
      RunState* state = reinterpret_cast<RunState*>(arg);
      MutexLock l(&state->mu);
      ASSERT_EQ(state->called, false);
      state->called = true;
      state->cvar.Signal();
    }
  };

  RunState state;
  MQSchedule mqs;

  env_->Schedule(&RunState::Run, &state);

  MutexLock l(&state.mu);
  while (!state.called) {
    state.cvar.Wait();
  }
}

struct Manager {
  MQSchedule* env;
  bool* done;
  port::CondVar* cv;

  bool* compaction_done;
  port::CondVar* compaction_cv;
};

static void LoadFilter(void* arg) {
  Manager* manager = static_cast<Manager*>(arg);
  // wake up compaction thread to use filer
  *manager->done = true;
  manager->cv->SignalAll();
}

static void Compaction(void* arg) {
  Manager* manager = static_cast<Manager*>(arg);
  // deadlock here
  // thread in schedule is working for compaction
  // just running "background_work_function(background_work_arg);" in BackgroundThreadMain
  // finished after LoadingFilter done
  manager->env->Schedule(LoadFilter, manager);

  //waiting for using reader
  while (!*(manager->done)) {
    manager->cv->Wait();
  }

  // wake up main thread if compaction is done
  *manager->compaction_done = true;
  manager->compaction_cv->SignalAll();
}

TEST_F(MQScheduleTest, DeadlockInMQ) {
  Manager manager;
  bool done = false;
  port::Mutex mutex;
  port::CondVar cv(&mutex);

  MQSchedule* mq_env = MQSchedule::Default();
  manager.done = &done;
  manager.env = mq_env;
  manager.cv = &cv;

  bool compaction_done = false;
  port::Mutex compaction_mutex;
  port::CondVar compaction_cv(&compaction_mutex);

  manager.compaction_done = &compaction_done;
  manager.compaction_cv = &compaction_cv;

  Env* env = Env::Default();
  env->Schedule(Compaction, &manager);

  // compaction thread never wake up
  while (!compaction_done) {
    compaction_cv.Wait();
  }
}
}