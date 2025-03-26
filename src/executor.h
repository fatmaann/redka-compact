#pragma once

#include "task.h"
#include "intrusive_queue.h"

namespace redka::io {
    class Acceptor;

    class Executor {
    public:
        explicit Executor(Acceptor* acceptor);

        void Schedule(ITask* task);

        void Run();

        static Executor* GetCur();

    private:
        Acceptor* acceptor_;
        detail::IntrusiveQueue<ITask> runq_;
    };
}
