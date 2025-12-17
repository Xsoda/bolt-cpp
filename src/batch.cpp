#include "batch.hpp"
#include "db.hpp"
#include "async.hpp"
#include <chrono>
#include <mutex>
#include <thread>
#include <iterator>
#include <cassert>

namespace bolt {

void AfterFunc(std::chrono::milliseconds delay, std::function<void()> &&fn) {
    AsyncFireAndForget([fn](std::chrono::milliseconds delay) {
        std::this_thread::sleep_for(delay);
        fn();
    }, delay);
}

void batch::trigger() { std::call_once(start, std::bind(&batch::run, this)); }

void batch::run() {
    auto dbptr = db.lock();
    if (!dbptr) {
        assert("DB already closed at batch" && false);
        return;
    }

    do {
            std::lock_guard<std::mutex> _(dbptr->batchMu);
            if (dbptr->batch.get() == this) {
                dbptr->batch = nullptr;
            }
    } while (0);

    while (calls.size() > 0) {
        int failIdx = -1;
        auto err = dbptr->Update([&](bolt::TxPtr tx) -> bolt::ErrorCode {
            for (auto it = calls.begin(); it != calls.end(); it++) {
                auto ret = it->get()->fn(tx);
                if (ret != bolt::ErrorCode::Success) {
                    failIdx = std::distance(calls.begin(), it);
                    return ret;
                }
            }
            return bolt::ErrorCode::Success;
        });

        if (failIdx >= 0) {
            auto c = std::move(calls[failIdx]);
            calls.erase(calls.begin() + failIdx);
            c->err.set_value(bolt::ErrorCode::ErrorTrySolo);
            continue;
        }
        for (auto &it : calls) {
            it->err.set_value(bolt::ErrorCode::Success);
        }
        break;
  }
}

}
