#pragma once

#include "task.h"

#include <cassert>
#include <coroutine>
#include <optional>
#include <utility>
#include <memory>

namespace redka::io {
    struct ThisCoroTag {};

    constexpr ThisCoroTag ThisCoro{};

    template <typename T>
    class CoroResult final : public ITask {
    public:
        struct promise_type {
            std::optional<T> value_;
            std::coroutine_handle<> cont_;
            CoroResult<T>* this_coro_{};
            auto get_return_object() {
                return CoroResult{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_always initial_suspend() noexcept { return {}; }

            auto&& await_transform(auto&& coro) {
                return std::forward<decltype(coro)>(coro);
            }

            auto await_transform(ThisCoroTag) {
                struct thiscoro_awaiter {
                    bool await_ready() noexcept {
                        return true;
                    }

                    void await_suspend(std::coroutine_handle<>) noexcept {
                        //Unreachable
                        std::terminate();
                    }

                    auto await_resume() noexcept {
                        assert(this_coro_);

                        return this_coro_;
                    }

                    CoroResult<T>* this_coro_{};
                };

                return thiscoro_awaiter(this_coro_);
            }

            struct final_awaiter {
              bool await_ready() noexcept {
                return false;
              }

              std::coroutine_handle<> await_suspend(auto h) noexcept {
                  if (h.promise().cont_) {
                    return h.promise().cont_;
                  } else {
                    return std::noop_coroutine();
                  }
              }
              void await_resume() noexcept {}
            };

            final_awaiter final_suspend() noexcept { return {}; }

            void return_value(T val) {
                value_.emplace(std::move(val));
            }

            void unhandled_exception() {
                std::terminate();
            }
        };

        bool await_ready() {
            return false;
        }

        auto await_suspend(std::coroutine_handle<> cont) noexcept {
            auto& promise = handle_.promise();

            promise.cont_ = cont;

            return handle_;
        }

        T await_resume() {
            return Get();
        }


        explicit CoroResult(std::coroutine_handle<promise_type> handle)
            : handle_(std::move(handle)) {
            handle_.promise().this_coro_ = this;
        }

        CoroResult(const CoroResult<T>&) = delete;
        CoroResult& operator=(const CoroResult<T>&) = delete;

        CoroResult(CoroResult<T>&& other) = delete;


        ~CoroResult() {
            if (handle_) {
                handle_.destroy();
            }
        }

        void Run() override {
            handle_();
        }

    private:
        T Get() {
            return std::move(*handle_.promise().value_);
        }

        std::coroutine_handle<promise_type> handle_;

    };

    template <>
    class CoroResult<void> final : public ITask {
        struct OnHeapTag{};
    public:
        struct promise_type {
            std::coroutine_handle<> cont_;
            CoroResult<void>* this_coro_{};
            bool need_destroy_ = false;
            auto get_return_object() {
                return CoroResult{std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_always initial_suspend() noexcept { return {}; }

            auto&& await_transform(auto&& coro) noexcept {
                return std::forward<decltype(coro)>(coro);
            }

            auto await_transform(ThisCoroTag) {
                struct thiscoro_awaiter {
                    bool await_ready() noexcept {
                        return true;
                    }

                    void await_suspend(std::coroutine_handle<>) noexcept {
                        //Unreachable
                        std::terminate();
                    }

                    auto await_resume() {
                        assert(this_coro_);

                        return this_coro_;
                    }

                    CoroResult<void>* this_coro_{};
                };

                return thiscoro_awaiter(this_coro_);
            }

            struct final_awaiter {
                  CoroResult<void>* to_destroy_{};
                  bool await_ready() noexcept {
                    return false;
                  }

                  std::coroutine_handle<> await_suspend(auto h) noexcept {
                      if (h.promise().cont_) {
                        auto cont = h.promise().cont_;
                          if (to_destroy_) {
                              delete to_destroy_;
                          }
                        return cont;
                      } else {
                          if (to_destroy_) {
                              delete to_destroy_;
                          }
                        return std::noop_coroutine();
                      }
                  }
                  void await_resume() noexcept {
                     //unreachable
                      std::terminate();
                  }
            };

            final_awaiter final_suspend() noexcept {
                if (need_destroy_) {
                    assert(this_coro_);
                    return final_awaiter{this_coro_};
                } else {
                    return final_awaiter{};
                }
            }

            void return_void() { }

            void unhandled_exception() {
                std::terminate();
            }
        };

        bool await_ready() noexcept {
            return false;
        }

        auto await_suspend(std::coroutine_handle<> cont) noexcept {
            auto& promise = handle_.promise();

            promise.cont_ = cont;

            return handle_;
        }

        void await_resume() {
        }


        explicit CoroResult(std::coroutine_handle<promise_type> handle)
            : handle_(std::move(handle)) {
            handle_.promise().this_coro_ = this;
        }

        CoroResult(const CoroResult<void>&) = delete;
        CoroResult& operator=(const CoroResult<void>&) = delete;

        CoroResult(CoroResult<void>&& other) = delete;

        CoroResult<void>* fire_and_forgive() && {
            return std::make_unique<CoroResult<void>>(std::move(*this), OnHeapTag{}).release();
        }

        CoroResult(CoroResult<void>&& other, OnHeapTag) noexcept
        : handle_(std::exchange(other.handle_, nullptr)) {
            handle_.promise().this_coro_ = this;
            handle_.promise().need_destroy_ = true;
        }

        CoroResult& operator=(CoroResult<void>&& other) = delete; //TODO

        ~CoroResult() {
            if (handle_) {
                handle_.destroy();
            }
        }

        void Run() override {
            handle_();
        }

    private:
        std::coroutine_handle<promise_type> handle_;

    };
}
