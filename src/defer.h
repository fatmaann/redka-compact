#pragma once

#include <concepts>
#include <functional>

namespace redka::io::detail {
    template <std::invocable F>
    class Defer {
    public:
        explicit Defer(const F& f)
            : f_(f) {
        }

        explicit Defer(F&& f)
            : f_(std::move(f)) {
        }

        Defer(Defer&&) = delete;
        Defer& operator=(Defer&&) = delete;
        Defer& operator=(const Defer&) = delete;
        Defer(const Defer&) = delete;

        ~Defer() {
            std::invoke(std::move(f_));
        }
    private:
        F f_;
    };
}
