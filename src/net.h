#pragma once

#include "coro_task.h"


#include <poll.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <span>
#include <memory>
#include <unordered_map>
#include <vector>

namespace redka::io {
    constexpr size_t kMaxFds = 1024;
    class Acceptor;


    class Executor;
    class TcpSocket {
    public:
        TcpSocket(Acceptor* acceptor, int fd)
            : parent_(acceptor)
            , fd_(fd) {
        }

        TcpSocket(const TcpSocket&) = delete;
        TcpSocket& operator=(const TcpSocket&) = delete;
        TcpSocket& operator=(TcpSocket&&) = delete;
        TcpSocket(TcpSocket&&) noexcept;

        ~TcpSocket();

        CoroResult<size_t> WriteSome(std::span<const char> view);

        CoroResult<size_t> WriteAll(std::span<const char> view);

        CoroResult<size_t> ReadSome(std::span<char> view);

        CoroResult<size_t> ReadAll(std::span<char> view);
    private:
        Acceptor* parent_;
        int fd_{};
    };

    class Acceptor {
        struct PrivateTag {};
        friend class TcpSocket;

        enum class EventType {
            read,
            write,
        };
    public:
        static std::unique_ptr<Acceptor> ListenOn(sockaddr_in addr);

        Acceptor(sockaddr_in addr, PrivateTag);
        CoroResult<TcpSocket> Accept();

        void RegisterRead(int fd, ITask* task) {
            RegisterEvent(EventType::read, fd, task);
        }
        void RegisterWrite(int fd, ITask* task) {
            RegisterEvent(EventType::write, fd, task);
        }

        void PollAll(Executor* executor);
        ~Acceptor();

    private:
        void BindListen();

        void RegisterEvent(EventType type, int fd, ITask* task);


    private:
        sockaddr_in addr_;
        bool opened_ = false;
        int serverfd_;

        struct PollEvents {
            ITask* read_cont{};
            ITask* write_cont{};
        };

        std::unordered_map<int, PollEvents> events_;
        std::vector<pollfd> pollfds_;
    };
}
