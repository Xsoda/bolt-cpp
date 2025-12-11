#include "db.hpp"
#include "batch.hpp"
#include "freelist.hpp"
#include "tx.hpp"
#include <mutex>
#include "defer.hpp"

namespace bolt {

bolt::ErrorCode DB::Batch(std::function<bolt::ErrorCode(bolt::TxPtr)> &&fn) {
    std::shared_ptr<bolt::call> c = std::make_shared<bolt::call>();
    do {
        std::lock_guard<std::mutex> lock(batchMu);
        if (batch == nullptr || (batch != nullptr
                                 && batch->calls.size() >= MaxBatchSize)) {
            batch = std::make_unique<bolt::batch>(shared_from_this());
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

bolt::ErrorCode DB::Update(std::function<bolt::ErrorCode(bolt::TxPtr)> &&fn) {
    auto [tx, err] = Begin(true);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    defer({
            if (!tx->db.expired()) {
                tx->rollback();
            }
        });

    tx->managed = true;
    err = fn(tx);
    tx->managed = false;

    if (err != bolt::ErrorCode::Success) {
        tx->Rollback();

        return err;
    }
    return tx->Commit();
}

bolt::ErrorCode DB::View(std::function<bolt::ErrorCode(bolt::TxPtr)> &&fn) {
    auto [tx, err] = Begin(false);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    defer({
            if (!tx->db.expired()) {
                tx->rollback();
            }
        });

    tx->managed = true;
    err = fn(tx);
    tx->managed = false;

    if (err != bolt::ErrorCode::Success) {
        tx->Rollback();
        return err;
    }
    return tx->Rollback();
}

std::tuple<bolt::TxPtr, bolt::ErrorCode> DB::Begin(bool writable) {
    if (writable) {
        return beginRWTx();
    }
    return beginTx();
}

std::tuple<bolt::TxPtr, bolt::ErrorCode> DB::beginTx() {
    metalock.lock();
    mmaplock.lock_shared();
    if (!opened) {
        mmaplock.unlock_shared();
        metalock.unlock();
        return std::make_tuple<bolt::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseNotOpen);

    }
    bolt::TxPtr tx = std::make_shared<bolt::Tx>(shared_from_this(), false);
    txs.push_back(tx);
    metalock.unlock();

    statlock.lock();
    stats.TxN++;
    stats.OpenTxN = txs.size();
    statlock.unlock();
    return std::make_tuple(tx, bolt::ErrorCode::Success);
}

std::tuple<bolt::TxPtr, bolt::ErrorCode> DB::beginRWTx() {
    if (readOnly) {
        return std::make_tuple<bolt::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseReadOnly);
    }
    rwlock.lock();
    std::lock_guard<std::mutex> _(metalock);
    if (!opened) {
        rwlock.unlock();
        return std::make_tuple<bolt::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseNotOpen);
    }
    std::shared_ptr<bolt::Tx> tx = std::make_shared<bolt::Tx>(shared_from_this(), true);
    rwtx = tx;
    bolt::txid minid = 0xFFFFFFFFFFFFFFFF;
    for (auto it : txs) {
        if (it->meta.txid < minid) {
            minid = it->meta.txid;
        }
    }
    if (minid > 0) {
        freelist->release(minid - 1);
    }
    return std::make_tuple(tx, bolt::ErrorCode::Success);
}

void DB::removeTx(bolt::TxPtr tx) {
    mmaplock.unlock_shared();

    metalock.lock();
    std::erase_if(txs, [&](bolt::TxPtr item) -> bool { return item == tx; });
    metalock.unlock();

    statlock.lock();
    stats.OpenTxN = txs.size();
    stats.TxStats += tx->Stats();
    statlock.unlock();
}

}
