#ifndef __BATCH_HPP__
#define __BATCH_HPP__

#include "error.hpp"
#include <chrono>
#include <functional>
#include <future>
#include <mutex>

namespace bolt {

struct DB;
struct Tx;

void AfterFunc(std::chrono::milliseconds delay, std::function<void()> &&fn);

struct call {
    std::function<bolt::ErrorCode(std::shared_ptr<bolt::Tx>)> fn;
    std::promise<bolt::ErrorCode> err;
    call() = default;
    call(const call &other) = delete;
    call(call &&other) = delete;
};

struct batch {
    std::weak_ptr<bolt::DB> db;
    std::once_flag start;
    std::vector<std::shared_ptr<call>> calls;

    batch(std::shared_ptr<bolt::DB> db) : db(db){};
    void trigger();
    void run();
};

};

#endif  // !__BATCH_HPP__
