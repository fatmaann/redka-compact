#pragma once

#include "intrusive_queue.h"

namespace redka::io {
    class ITask : public detail::Intrusive<ITask> {
    public:
        void operator()() {
            Run();
        }

        virtual void Run() = 0;

        virtual ~ITask() = default;
    };
}
