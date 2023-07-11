//
// Created by WangTingZheng on 2023/7/11.
//

#include "leveldb/env.h"
#include "port/port.h"

#include <gtest/gtest.h>

namespace leveldb {
  struct Controller {
    Env* env;
    bool* done;
    port::CondVar* cv;

    bool* compaction_done;
    port::CondVar* compaction_cv;
  };

  static void LoadFilter(void* arg) {
    Controller* controller = static_cast<Controller*>(arg);
    *controller->done = true;
    controller->cv->SignalAll();
  }

  static void Compaction(void* arg) {
    Controller* controller = static_cast<Controller*>(arg);
    // deadlock here
    // thread in schedule is working for compaction
    // just running "background_work_function(background_work_arg);" in BackgroundThreadMain
    // finished after LoadingFilter done
    controller->env->Schedule(LoadFilter, controller);

    while (!*(controller->done)) {
      controller->cv->Wait();
    }

    // wake up main thread if compaction is done
    *controller->compaction_done = true;
    controller->compaction_cv->SignalAll();
  }

  TEST(EnvTest, DeadlockInMQ) {
    Controller controller;
    Env* env = Env::Default();
    bool done = false;
    port::Mutex mutex;
    port::CondVar cv(&mutex);

    controller.done = &done;
    controller.env = env;
    controller.cv = &cv;

    bool compaction_done = false;
    port::Mutex compaction_mutex;
    port::CondVar compaction_cv(&compaction_mutex);

    controller.compaction_done = &compaction_done;
    controller.compaction_cv = &compaction_cv;

    env->Schedule(Compaction, &controller);

    // compaction thread never wake up
    while (!compaction_done) {
      compaction_cv.Wait();
    }
  }
}