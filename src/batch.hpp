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

struct timer {
    std::once_flag once;
    std::function<void()> func;
    void AfterFunc(std::chrono::milliseconds delay, std::function<void()> &&fn);
};

struct call {
    std::function<bolt::ErrorCode(bolt::Tx *)> fn;
    std::promise<bolt::ErrorCode> err;
    call() = default;
    call(const call &other) = delete;
    call(call &&other) = delete;
};

struct batch {
    bolt::DB *db;
    std::once_flag start;
    std::vector<std::shared_ptr<call>> calls;
    bolt::timer timer;

    batch(bolt::DB *db) : db(db){};
    void trigger();
    void run();
};

};

#endif  // !__BATCH_HPP__
