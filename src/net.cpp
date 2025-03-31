#include "net.h"

#include <cassert>
#include <ranges>
#include <stdexcept>

#include "executor.h"

namespace redka::io {
CoroResult<size_t> TcpSocket::WriteAll(std::span<const char> view) {
    size_t all_written = 0;
    while (!view.empty()) {
        size_t num_written = co_await WriteSome(view);
        view = view.subspan(num_written);
        all_written += num_written;
    }

    co_return all_written;
}

CoroResult<size_t> TcpSocket::WriteSome(std::span<const char> view) {
    CoroResult<size_t>* this_coro = co_await ThisCoro;
    parent_->RegisterWrite(fd_, this_coro);
    co_await std::suspend_always{};
    co_return send(fd_, view.data(), view.size(), 0);
}

CoroResult<size_t> TcpSocket::ReadAll(std::span<char> view) {
    size_t all_read = 0;
    while (!view.empty()) {
        size_t num_read = co_await ReadSome(view);
        view = view.subspan(num_read);
        all_read += num_read;
    }

    co_return all_read;
}

CoroResult<size_t> TcpSocket::ReadSome(std::span<char> view) {
    CoroResult<size_t>* this_coro = co_await ThisCoro;
    parent_->RegisterRead(fd_, this_coro);
    co_await std::suspend_always{};
    co_return recv(fd_, view.data(), view.size(), 0);
}

TcpSocket::TcpSocket(TcpSocket&& other) noexcept
    : parent_(std::exchange(other.parent_, nullptr)), fd_(std::exchange(other.fd_, 0)) {
}

TcpSocket::~TcpSocket() {
    if (fd_) {
        close(fd_);
    }

    if (parent_) {
        parent_->events_.erase(fd_);
    }
}

Acceptor::~Acceptor() {
    if (opened_) {
        close(serverfd_);
    }
}

Acceptor::Acceptor(sockaddr_in addr, Acceptor::PrivateTag) : addr_(addr) {
}

CoroResult<TcpSocket> Acceptor::Accept() {
    assert(opened_);
    CoroResult<TcpSocket>* this_coro = co_await ThisCoro;
    RegisterRead(serverfd_, this_coro);
    co_await std::suspend_always{};
    sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);
    int client_fd;

    if ((client_fd = accept(serverfd_, (sockaddr*)&client_addr, &addr_len)) < 0) {
        throw std::runtime_error{"accept failed"};
    }

    co_return TcpSocket(this, client_fd);
}

void Acceptor::RegisterEvent(Acceptor::EventType type, int fd, ITask* task) {
    using enum EventType;
    auto& poll_event = events_[fd];
    ITask** cont{};
    switch (type) {
        case read:
            cont = &poll_event.read_cont;
            break;
        case write:
            cont = &poll_event.write_cont;
            break;
    }

    // cont is filled
    assert(cont);

    // cont is empty
    assert(!*cont);

    *cont = task;
}

void Acceptor::PollAll(Executor* executor) {
    namespace rv = std::ranges::views;
    assert(executor);

    pollfds_.clear();

    // TODO fixed array of fds, just turn unused to -1
    std::ranges::copy(events_ | rv::filter([](const auto& kv) {
                          const auto& events = kv.second;
                          return events.read_cont || events.write_cont;
                      }) | rv::transform([](const auto& kv) {
                          pollfd pfd;
                          pfd.fd = kv.first;
                          pfd.events = 0;
                          pfd.revents = 0;
                          const auto& events = kv.second;
                          if (events.read_cont) {
                              pfd.events |= POLLIN;
                          }
                          if (events.write_cont) {
                              pfd.events |= POLLOUT;
                          }

                          return pfd;
                      }),
                      std::back_inserter(pollfds_));

    poll(pollfds_.data(), pollfds_.size(), -1);

    for (auto pfd : pollfds_) {
        auto& pevent = events_[pfd.fd];

        // TODO handle err and hang up
        //
        if (pfd.revents & POLLOUT) {
            assert(pevent.write_cont);
            executor->Schedule(std::exchange(pevent.write_cont, nullptr));
        }

        if (pfd.revents & (POLLIN)) {
            assert(pevent.read_cont);
            executor->Schedule(std::exchange(pevent.read_cont, nullptr));
        }
    }
}

void Acceptor::BindListen() {
    if ((serverfd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1) {
        throw std::runtime_error{"socket creation failed"};
    }

    opened_ = true;

    int opt = 1;
    if (setsockopt(serverfd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        throw std::runtime_error{"setsockopt failed"};
    }

    if (bind(serverfd_, (struct sockaddr*)&addr_, sizeof(addr_)) < 0) {
        throw std::runtime_error{"bind failed"};
    }

    if (listen(serverfd_, 128) < 0) {
        throw std::runtime_error{"listen failed"};
    }
}

std::unique_ptr<Acceptor> Acceptor::ListenOn(sockaddr_in addr) {
    auto acceptor = std::make_unique<Acceptor>(addr, PrivateTag{});
    acceptor->BindListen();
    return acceptor;
}
}  // namespace redka::io
