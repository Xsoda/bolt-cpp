#include "db.hpp"
#include "batch.hpp"
#include "common.hpp"
#include "file.hpp"
#include "freelist.hpp"
#include "tx.hpp"
#include "page.hpp"
#include <mutex>
#include "async.hpp"
#include <cassert>
#include <tuple>
#include <unistd.h>

namespace bolt {

DB::DB() {
    freelist = std::make_unique<bolt::freelist>();
}

bolt::ErrorCode DB::init() {
    // Set the page size to the OS page size.
    pageSize = platform::Getpagesize();

    // Create two meta pages on a buffer.
    std::vector<std::byte> buf;
    buf.assign(pageSize * 4, (std::byte)0);
    for (int i = 0; i < 2; i++) {
        bolt::page *p = pageInBuffer(buf, i);
        p->id = i;
        p->flags = metaPageFlag;

        // Initialize the meta page.
        auto m = p->meta();
        m->magic = bolt::magic;
        m->version = bolt::version;
        m->pageSize = (std::uint32_t)pageSize;
        m->freelist = 2;
        m->root.root = 3;
        m->pgid = 4;
        m->txid = i;
        m->checksum = m->sum64();
    }

    // Write an empty freelist at page 3.
    bolt::page *p = pageInBuffer(buf, (bolt::pgid)2);
    p->id = 2;
    p->flags = bolt::freeListPageFlag;
    p->count = 0;

    // Write an empty leaf page at page 4.
    p = pageInBuffer(buf, bolt::pgid(3));
    p->id = 3;
    p->flags = bolt::leafPageFlag;
    p->count = 0;

    auto [_, err] = file.WriteAt(buf, 0);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    return file.Fdatasync();
}

bolt::ErrorCode DB::Batch(std::function<bolt::ErrorCode(bolt::TxPtr)> &&fn) {
    std::shared_ptr<bolt::call> c = std::make_shared<bolt::call>();
    do {
        std::lock_guard<std::mutex> lock(batchMu);
        if (batch == nullptr || (batch != nullptr
                                 && batch->calls.size() >= MaxBatchSize)) {
            batch = std::make_unique<bolt::batch>(shared_from_this());
            AfterFunc(MaxBatchDelay, [&]() {
                batch->trigger();
            });
        }
        c->fn = std::move(fn);
        batch->calls.push_back(c);
        if (batch->calls.size() >= MaxBatchSize) {
            AsyncFireAndForget([&]() {
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

    tx->managed = true;
    err = fn(tx);
    tx->managed = false;

    if (err != bolt::ErrorCode::Success) {
        tx->Rollback();

        if (!tx->db.expired()) {
            tx->rollback();
        }

        return err;
    }
    err = tx->Commit();

    if (!tx->db.expired()) {
        tx->rollback();
    }

    return err;
}

bolt::ErrorCode DB::View(std::function<bolt::ErrorCode(bolt::TxPtr)> &&fn) {
    auto [tx, err] = Begin(false);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    tx->managed = true;
    err = fn(tx);
    tx->managed = false;

    if (err != bolt::ErrorCode::Success) {
        tx->Rollback();

        if (!tx->db.expired()) {
            tx->rollback();
        }

        return err;
    }
    err = tx->Rollback();

    if (!tx->db.expired()) {
        tx->rollback();
    }

    return err;
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
    // If the database was opened with Options.ReadOnly, return an error.
    if (readOnly) {
        return std::make_tuple<bolt::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseReadOnly);
    }
    // Obtain writer lock. This is released by the transaction when it closes.
    // This enforces only one writer transaction at a time.
    rwlock.lock();

    // Once we have the writer lock then we can lock the meta pages so that
    // we can set up the transaction.
    std::lock_guard<std::mutex> _(metalock);

    // Exit if the database is not open yet.
    if (!opened) {
        rwlock.unlock();
        return std::make_tuple<bolt::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseNotOpen);
    }

    // Create a transaction associated with the database.
    std::shared_ptr<bolt::Tx> tx =
        std::make_shared<bolt::Tx>(shared_from_this(), true);
    rwtx = tx;

    // Free any pages associated with closed read-only transactions.
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

// meta retrieves the current meta page reference.
bolt::meta *DB::meta() {
    // We have to return the meta with the highest txid which doesn't fail
    // validation. Otherwise, we can cause errors when in fact the database is
    // in a consistent state. metaA is the one with the higher txid.
    auto metaA = meta0;
    auto metaB = meta1;
    if (meta1->txid > meta0->txid) {
        metaA = meta1;
        metaB = meta0;
    }

    // Use higher meta page if valid. Otherwise fallback to previous, if valid.
    if (!metaA->validate()) {
        return metaA;
    } else if (!metaB->validate()) {
        return metaB;
    }

    // This should never be reached, because both meta1 and meta0 were validated
    // on mmap() and we do fsync() on every write.
    assert("bolt.DB.meta(): invalid meta pages" && false);
    return nullptr;
}

bolt::page *DB::page(bolt::pgid id) {
    auto pos = id * (bolt::pgid)pageSize;
    return reinterpret_cast<bolt::page *>(&((std::byte *)dataref)[pos]);
}

bolt::page *DB::pageInBuffer(bolt::bytes b, bolt::pgid id) {
    return reinterpret_cast<bolt::page *>(&b[id *(bolt::pgid)pageSize]);
}

// grow grows the size of the database to the given sz.
bolt::ErrorCode DB::grow(int sz) {
    if (sz < filesz) {
        return bolt::ErrorCode::Success;
    }

    // If the data is smaller than the alloc size then only allocate what's
    // needed. Once it goes over the allocation size then allocate in chunks.
    if (datasz < AllocSize) {
        sz = datasz;
    } else {
        sz += AllocSize;
    }

    // Truncate and fsync to ensure file size metadata is flushed.
    // https://github.com/boltdb/bolt/issues/284
    if (!NoGrowSync && !readOnly) {
        auto err = file.Truncate(sz);
        if (err != bolt::ErrorCode::Success) {
            return bolt::ErrorCode::ErrorFileResizeFail;
        }
        err = file.Fsync();
        if (err != bolt::ErrorCode::Success) {
            return bolt::ErrorCode::ErrorFileSyncFail;
        }
    }
    filesz = sz;
    return bolt::ErrorCode::Success;
}

}
