#pragma once

#include <cassert>
#include <concepts>

namespace redka::io::detail {
    template <typename T>
    struct Intrusive {
    public:
        T& operator*() noexcept {
            return *Get();
        }

        const T& operator*() const noexcept {
            return *Get();
        }

        T* Get() noexcept {
            return static_cast<T*>(this);
        }

        const T* Get() const noexcept {
            return static_cast<const T*>(this);
        }

        bool IsLinked() const noexcept {
            return next_ != nullptr;
        }

        void LinkBefore(Intrusive<T>* next) noexcept {
            assert(!IsLinked());

            prev_ = next->prev_;
            prev_->next_ = this;
            next_ = next;
            next->prev_ = this;
        }

        void Unlink() {
            if (next_) {
                next_->prev_ = prev_;
            }

            if (prev_) {
                prev_->next_ = next_;
            }

            prev_ = next_ = nullptr;
        }


        Intrusive<T>* prev_{};
        Intrusive<T>* next_{};
    };

    template <typename T>
    requires (std::derived_from<T, Intrusive<T>>)
    class IntrusiveQueue {
        using Node = Intrusive<T>;
    public:
        IntrusiveQueue() {
            head_.prev_ = head_.next_ = &head_;
        }

        void Push(Node* elem) {
            elem->LinkBefore(&head_);
        }

        T* Pop() noexcept {
            if (Empty()) {
                return nullptr;
            }

            Node* front = head_.next_;
            front->Unlink();
            return front->Get();
        }

        bool Empty() const noexcept {
            return head_.next_ == &head_;
        }
    private:
        Intrusive<T> head_{};

    };
}
