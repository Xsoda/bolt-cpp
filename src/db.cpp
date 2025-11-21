#include "db.hpp"
#include "batch.hpp"
#include <mutex>

namespace bolt {

bolt::ErrorCode DB::Batch(std::function<bolt::ErrorCode(bolt::Tx *)> &&fn) {
    std::shared_ptr<bolt::call> c = std::make_shared<bolt::call>();
    do {
        std::lock_guard<std::mutex> _(batchMu);
        if (batch == nullptr || (batch != nullptr
                                 && batch->calls.size() >= MaxBatchSize)) {
            batch = std::make_unique<bolt::batch>(this);
            batch->timer.AfterFunc(MaxBatchDelay, [&]() {
                batch->trigger();
            });
        }
        c->fn = std::move(fn);
        batch->calls.push_back(c);
        if (batch->calls.size() >= MaxBatchSize) {
            std::ignore = std::async(std::launch::async, [&]() {
                batch->trigger();
            });
        }
    } while (0);

    auto f = c->err.get_future();
    f.wait();

    if (f.get() == bolt::ErrorCode::ErrorTrySolo) {
        return Update(std::move(c->fn));
    }
    return bolt::ErrorCode::Success;
}

}
