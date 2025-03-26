#include "executor.h"
#include "net.h"

#include <cassert>


namespace redka::io {
    static Executor* cur_executor = nullptr;

    Executor::Executor(Acceptor* acceptor)
        : acceptor_(acceptor) {
    }

    void Executor::Schedule(ITask* task) {
        runq_.Push(task);
    }

    Executor* Executor::GetCur() {
        assert(cur_executor);

        return cur_executor;
    }

    void Executor::Run() {
        while (true) {
            ITask* task;
            while ((task = runq_.Pop())) {
                cur_executor = this;
                task->Run();
                cur_executor = nullptr;
            }

            acceptor_->PollAll(this);
            assert(!runq_.Empty());
        }
    }
}
