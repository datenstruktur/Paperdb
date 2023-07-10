//
// Created by WangTingZheng on 2023/7/10.
//
#include "mq_schedule.h"
#include <gtest/gtest.h>
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
}