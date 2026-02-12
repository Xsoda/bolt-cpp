#include "impl/batch.hpp"
#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/async.hpp"
#include <chrono>
#include <mutex>
#include <thread>
#include <iterator>
#include "fmt/std.h"

namespace bolt::impl {

void batch::trigger() {
    std::call_once(start, std::bind(&batch::run, this));
}

void batch::run() {
    auto dbptr = db.lock();
    if (!dbptr) {
        _assert(false, "DB already closed at batch");
        return;
    }

    if (timer.joinable()) {
        timer.request_stop();
    }

    do {
        std::lock_guard<std::mutex> lock(dbptr->batchMu);
        if (dbptr->batch.get() == this) {
            fmt::println("batch user count: {}, {}", dbptr->batch.use_count(), fmt::ptr(this));
            dbptr->batch = nullptr;
        }
    } while (0);

    while (calls.size() > 0) {
        ptrdiff_t failIdx = -1;
        auto err = dbptr->Update([&](impl::TxPtr tx) -> bolt::ErrorCode {
            for (auto it = calls.begin(); it != calls.end(); it++) {
                bolt::ErrorCode err;
                try {
                    err = it->get()->fn(tx);
                } catch ([[maybe_unused]] const std::exception &e) {
                    err = bolt::ErrorExceptionCaptured;
                }
                if (err != bolt::Success) {
                    failIdx = std::distance(calls.begin(), it);
                    return err;
                }
            }
            return bolt::Success;
        });

        if (failIdx >= 0) {
            auto c = std::move(calls[failIdx]);
            calls.erase(calls.begin() + failIdx);
            c->err.set_value(bolt::ErrorTrySolo);
            continue;
        }
        for (auto &it : calls) {
            it->err.set_value(bolt::Success);
        }
        break;
    }
}

batch::~batch() {
    db.reset();
}

void batch::AfterFunc(std::chrono::milliseconds delay,
                      std::function<void()> &&fn) {
    timer = std::jthread([delay, fn, this](std::stop_token stoken) {
        auto until = std::chrono::steady_clock::now() + delay;
        do {
            if (stoken.stop_requested()) {
                return;
            }
            std::this_thread::sleep_for(1ms);
        } while (std::chrono::steady_clock::now() < until);
        fmt::println("delay timeout, {}", fmt::ptr(this));
        fn();
    });
}

}
