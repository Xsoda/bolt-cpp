#ifndef __BATCH_HPP__
#define __BATCH_HPP__

#include "impl/utils.hpp"
#include <chrono>
#include <functional>
#include <future>
#include <mutex>

namespace bolt::impl {

struct DB;
struct Tx;

void AfterFunc(std::chrono::milliseconds delay, std::function<void()> &&fn);

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

    batch(std::shared_ptr<impl::DB> db) : db(db){};
    void trigger();

    ~batch();
private:
    void run();
};

};

#endif  // !__BATCH_HPP__
