#ifndef __BATCH_HPP__
#define __BATCH_HPP__

#include "impl/utils.hpp"
#include <chrono>
#include <functional>
#include <future>
#include <mutex>
#include <thread>

namespace bolt::impl {

struct DB;
struct Tx;
struct batch;

struct call {
    std::function<bolt::ErrorCode(std::shared_ptr<impl::Tx>)> fn;
    std::promise<bolt::ErrorCode> err;

    call() = default;
    call(const call &other) = delete;
    call(call &&other) = delete;
};

struct batch {
    std::weak_ptr<impl::DB> db;
    std::once_flag start;
    std::vector<std::shared_ptr<call>> calls;

    std::jthread timer;

    batch(std::shared_ptr<impl::DB> db) : db(db){};
    void trigger();
    void wait();
    void AfterFunc(std::chrono::milliseconds delay, std::function<void()> &&fn);

    ~batch();

private:
    void run();
};

}; // namespace bolt::impl

#endif // !__BATCH_HPP__
