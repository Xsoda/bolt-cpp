#include "batch.hpp"
#include "db.hpp"
#include <chrono>
#include <mutex>
#include <thread>
#include <iterator>

namespace bolt {

void timer::AfterFunc(std::chrono::milliseconds delay,
                      std::function<void()> &&fn) {
  std::call_once(once, [&]() {
      func = std::move(fn);
      std::ignore = std::async(std::launch::async, [this](std::chrono::milliseconds delay) {
          std::this_thread::sleep_for(delay);
          this->func();
      }, delay);
  });
}

void batch::trigger() { std::call_once(start, std::bind(&batch::run, this)); }

void batch::run() {
    do {
        std::lock_guard<std::mutex> _(db->batchMu);
        if (db->batch.get() == this) {
            db->batch = nullptr;
        }
    } while (0);

    while (calls.size() > 0) {
        int failIdx = -1;
        auto err = db->Update([&](bolt::Tx *tx) -> bolt::ErrorCode {
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
