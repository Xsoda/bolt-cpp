#include "impl/batch.hpp"
#include "bolt/error.hpp"
#include "impl/db.hpp"
#include <chrono>
#include <iterator>
#include <mutex>
#include <thread>

namespace bolt::impl {

void batch::trigger() { std::call_once(start, std::bind(&batch::run, this)); }

void batch::run() {
    auto dbptr = db.lock();
    if (!dbptr) {
        bolt__assert(false, "DB already closed at batch");
        return;
    }

    do {
        std::lock_guard<std::mutex> lock(dbptr->batchMu);

        timer.request_stop();

        if (dbptr->batch.get() == this) {
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
                    err = bolt::ErrorCode::ExceptionCaptured;
                }
                if (err != bolt::ErrorCode::Success) {
                    failIdx = std::distance(calls.begin(), it);
                    return err;
                }
            }
            return bolt::ErrorCode::Success;
        });

        if (failIdx >= 0) {
            auto c = std::move(calls[failIdx]);
            calls.erase(calls.begin() + failIdx);
            c->err.set_value(bolt::ErrorCode::TrySolo);
            continue;
        }
        for (auto &it : calls) {
            it->err.set_value(bolt::ErrorCode::Success);
        }
        break;
    }
}

batch::~batch() { db.reset(); }

} // namespace bolt::impl
