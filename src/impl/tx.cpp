#include "impl/tx.hpp"
#include "impl/page.hpp"
#include "impl/bucket.hpp"
#include "impl/db.hpp"
#include "impl/meta.hpp"
#include "impl/freelist.hpp"
#include "impl/utils.hpp"
#include <algorithm>
#include <chrono>
#include <mutex>
#include <fmt/format.h>
#include <inttypes.h>

namespace bolt::impl {

Tx::Tx(impl::DBPtr db, impl::meta meta) : db(db), meta(meta) {}

Tx::Tx() {}

Tx::Tx(std::shared_ptr<impl::DB> db, bool writable)
    : db(db), writable(writable) {
}

void Tx::init() {
    auto dbptr = db.lock();
    dbptr->meta()->copy(&meta);
    managed = false;
    root = std::make_shared<impl::Bucket>(shared_from_this());
    root->root = meta.root.root;
    root->sequence = meta.root.sequence;
    if (writable) {
        meta.txid += 1;
    }
}

std::shared_ptr<impl::DB> Tx::DB() const { return db.lock(); }

impl::txid Tx::ID() const { return meta.txid; }

std::int64_t Tx::Size() const {
    if (auto dbptr = db.lock()) {
        return meta.pgid * dbptr->pageSize;
    }
    _assert(false, "Tx already closed");
    return 0;
}

bool Tx::Writable() const { return writable; }

impl::TxStats Tx::Stats() const { return stats; }

impl::page *Tx::page(impl::pgid id) {
    auto it = pages.find(id);
    if (it != pages.end()) {
        return it->second;
    }
    if (auto dbptr = db.lock()) {
        return dbptr->page(id);
    }
    _assert(false, "Tx already closed");
    return nullptr;
}

bolt::ErrorCode Tx::writeMeta() {
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorTxClosed;
    }
    std::vector<std::byte> buf;
    buf.assign(dbptr->pageSize, std::byte(0x00));

   impl::page *p = dbptr->pageInBuffer(bolt::bytes{buf}, 0);
    meta.write(p);
    auto [_, err] = dbptr->file.WriteAt(bolt::bytes{buf},
                                        (std::int64_t)p->id * dbptr->pageSize);
    if (err != bolt::Success) {
        return err;
    }

    if (!dbptr->NoSync) {
        err = dbptr->file.Fdatasync();
        if (err != bolt::Success) {
            return err;
        }
    }
    stats.Write++;
    return bolt::Success;
}

bolt::ErrorCode Tx::write() {
    // Sort pages by id.
    std::vector<impl::page *> pages;
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorTxClosed;
    }
    pages.reserve(this->pages.size());
    for (auto [key, value] : this->pages) {
        pages.push_back(value);
    }

    // Clear out page cache early.
    this->pages.clear();

    std::sort(pages.begin(), pages.end(),
              [&](impl::page *a, impl::page *b) { return a->id < b->id; });

    // Write pages to disk in order.
    for (auto it : pages) {
        auto size = int(it->overflow + 1) * dbptr->pageSize;
        auto offset = std::int64_t(it->id) * dbptr->pageSize;

        // Write out page in "max allocation" sized chunks.
        auto ptr = reinterpret_cast<std::byte *>(it);
        while (true) {
            // Limit our write to our max allocation size.
            auto sz = size;
            if (sz > impl::maxAllocSize - 1) {
                sz = impl::maxAllocSize - 1;
            }
            // Write chunk to disk.
            auto [_, err] = dbptr->file.WriteAt(bolt::bytes(ptr, sz), offset);
            if (err != bolt::Success) {
                return err;
            }
            // Update statistics.
            stats.Write++;

            // Exit inner for loop if we've written all the chunks.
            size -= sz;
            if (size == 0) {
                break;
            }
            // Otherwise move offset forward and move pointer to next chunk.
            offset += sz;
            ptr = &ptr[sz];
        }
    }

    // Ignore file sync if flag is set on DB.
    if (!dbptr->NoSync) {
        auto err = dbptr->file.Fdatasync();
        if (err != bolt::Success) {
            return err;
        }
    }
    // Put small pages back to page pool.
    for (auto it : pages) {
        dbptr->releasePage(it);
    }
    return bolt::Success;
}

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs, or if Commit is
// called on a read-only transaction.
bolt::ErrorCode Tx::Commit() {
    std::vector<impl::node_ptr> hold;
    _assert(!managed, "managed tx commit not allowed");
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorTxClosed;
    } else if (!writable) {
        return bolt::ErrorTxNotWritable;
    }

    // Rebalance nodes which have had deletions.
    auto startTime = std::chrono::system_clock::now();
    auto since = [](std::chrono::time_point<std::chrono::system_clock> start) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                                                                     std::chrono::system_clock::now() - start);
    };
    root->rebalance();
    if (stats.Rebalance > 0) {
        stats.RebalanceTime += since(startTime);
    }
    // spill data onto dirty pages.
    startTime = std::chrono::system_clock::now();
    if (auto err = root->spill(hold); err != bolt::Success) {
        rollback();
        return err;
    }
    stats.SpillTime += since(startTime);

    // Free the old root bucket.
    meta.root.root = root->root;

    auto opgid = meta.pgid;

    // Free the freelist and allocate new pages for it. This will overestimate
    // the size of the freelist but not underestimate the size (which would be
    // bad).
    dbptr->freelist->free(meta.txid, dbptr->page(meta.freelist));
    auto [p, ret] = allocate((dbptr->freelist->size() / dbptr->pageSize) + 1);
    if (ret != bolt::Success) {
        rollback();
        return ret;
    }
    if (auto err = dbptr->freelist->write(p); err != bolt::Success) {
        rollback();
        return err;
    }

    meta.freelist = p->id;

    // If the high water mark has moved up then attempt to grow the database.
    if (meta.pgid > opgid) {
        if (auto err = dbptr->grow((meta.pgid + 1) * dbptr->pageSize);
            err != bolt::Success) {
            return err;
        }
    }

    // Write dirty pages to disk.
    startTime = std::chrono::system_clock::now();
    if (auto err = write(); err != bolt::Success) {
        rollback();
        return err;
    }

    // If strict mode is enabled then perform a consistency check.
    // Only the first consistency error is reported in the panic.
    if (dbptr->StrictMode) {
        auto result = Check();
        auto errors = result.get();
        if (errors.size() > 0) {
            log_debug("database path: {}", dbptr->Path());
            for (auto &errstr : errors) {
                log_debug("  - check fail: {}", errstr);
            }
        }
        // _assert(errors.empty(), "check fail");
    }

    // Write meta to disk.
    if (auto err = writeMeta(); err != bolt::Success) {
        rollback();
        return err;
    }
    stats.WriteTime += since(startTime);
    // Finalize the transaction.
    close();
    // Execute commit handlers now that the locks have been removed.
    for (auto &fn : commitHandlers) {
        fn();
    }
    return bolt::Success;
}

bolt::ErrorCode Tx::Rollback() {
    _assert(!managed, "managed tx rollback not allowed");
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorTxClosed;
    }
    rollback();
    return bolt::Success;
}

void Tx::rollback() {
    auto dbptr = db.lock();
    if (!dbptr) {
        return;
    }
    if (writable) {
        dbptr->freelist->rollback(meta.txid);
        dbptr->freelist->reload(dbptr->page(dbptr->meta()->freelist));
    }
    close();
}

void Tx::close() {
    auto dbptr = db.lock();
    if (!dbptr) {
        return;
    }
    if (writable) {
        size_t freelistFreeN = dbptr->freelist->free_count();
        size_t freelistPendingN = dbptr->freelist->pending_count();
        size_t freelistAlloc = dbptr->freelist->size();

        dbptr->rwtx = nullptr;
        dbptr->rwlock.unlock();

        std::unique_lock<std::shared_mutex> lock(dbptr->statlock);
        dbptr->stats.FreePageN = freelistFreeN;
        dbptr->stats.PendingPageN = freelistPendingN;
        dbptr->stats.FreeAlloc = (freelistFreeN + freelistPendingN) * dbptr->pageSize;
        dbptr->stats.FreelistInuse = freelistAlloc;
        dbptr->stats.TxStats += stats;
    } else {
        dbptr->removeTx(shared_from_this());
    }

    db.reset();
    pages.clear();
}

// allocate returns a contiguous block of memory starting at a given page.
std::tuple<impl::page *, bolt::ErrorCode> Tx::allocate(size_t count) {
    auto dbptr = db.lock();
    if (!dbptr) {
        return std::make_tuple(nullptr, bolt::ErrorTxClosed);
    }
    auto [p, err] = dbptr->allocate(count);
    pages[p->id] = p;
    stats.PageCount++;
    stats.PageAlloc += count * dbptr->pageSize;
    return std::make_tuple(p, bolt::Success);
}

std::future<std::vector<std::string>> Tx::Check() {
    auto fn = [this]() -> std::vector<std::string> {
        std::map<impl::pgid, bool> freed;
        std::vector<impl::pgid> all;
        std::vector<std::string> errors;
        auto dbptr = db.lock();
        if (!dbptr) {
            errors.push_back("tx already closed");
            return errors;
        }
        // Check if any pages are double freed.
        all.assign(dbptr->freelist->count(), impl::pgid(0));
        dbptr->freelist->copyall(all);
        for (auto item : all) {
            auto it = freed.find(item);
            if (it != freed.end()) {
                errors.push_back(fmt::format("page {}: already freed", item));
            }
            freed[item] = true;
        }
        // Track every reachable page.
        std::map<impl::pgid, impl::page *> reachable;
        reachable[0] = page(0);
        reachable[1] = page(1);
        for (std::uint32_t i = 0; i <= page(meta.freelist)->overflow; i++) {
            reachable[meta.freelist + i] = page(meta.freelist);
        }

        // Recursively check buckets.
        checkBucket(root, reachable, freed, errors);

        // Ensure all pages below high water mark are either reachable or freed.
        for (impl::pgid i = 0; i < meta.pgid; i++) {
            auto it = reachable.find(i);
            auto itf = freed.find(i);
            if (it == reachable.end() && itf == freed.end()) {
                errors.push_back(fmt::format("page {}: unreachable unfreed", i));
            }
        }
        return errors;
    };
    return std::async(fn);
}

void Tx::checkBucket(impl::BucketPtr bucket,
                     std::map<impl::pgid, impl::page *> &reachable,
                     std::map<impl::pgid, bool> &freed,
                     std::vector<std::string> &errors) {
    // Ignore inline buckets.
    if (bucket->root == 0) {
        return;
    }
    auto txptr = bucket->tx.lock();
    if (!txptr) {
        return;
    }
    // Check every page used by this bucket.
    txptr->forEachPage(
        bucket->root, 0, [&](impl::page *p, int depth) {
            if (p->id > txptr->meta.pgid) {
                errors.push_back(fmt::format("page {}: out of bounds: {}", p->id, txptr->meta.pgid));
            }
            // Ensure each page is only referenced once.
            for (impl::pgid i = 0; i <= p->overflow; i++) {
                auto id = p->id + i;
                auto it = reachable.find(id);
                if (it != reachable.end()) {
                    errors.push_back(fmt::format("page {}: multiple references", id));
                }
                reachable[id] = p;
            }

            // We should only encounter un-freed leaf and branch pages.
            auto it = freed.find(p->id);
            if (it != freed.end()) {
                errors.push_back(fmt::format("page {}: reachable freed", p->id));
            } else if ((p->flags & impl::branchPageFlag) == 0 &&
                       (p->flags & impl::leafPageFlag) == 0) {
                errors.push_back(fmt::format("page {}: invalid type: {}", p->id, p->type()));
            }
        });

    // Check each bucket within this bucket.
    bucket->ForEach([&](bolt::const_bytes key, bolt::const_bytes val) -> bolt::ErrorCode {
        auto child = bucket->RetrieveBucket(key);
        if (child) {
            checkBucket(child, reachable, freed, errors);
        }
        return bolt::Success;
    });
}

void Tx::forEachPage(impl::pgid pgid, int depth,
                     std::function<void(impl::page *, int)> &&fn) {
    auto p = page(pgid);

    // Execute function.
    fn(p, depth);

    // Recursively loop over children.
    if (p->flags & impl::branchPageFlag) {
        for (std::uint16_t i = 0; i < p->count; i++) {
            auto elem = p->branchPageElement(i);
            forEachPage(elem->pgid, depth + 1, std::forward<decltype(fn)>(fn));
        }
    }
}

std::tuple<impl::BucketPtr, bolt::ErrorCode>
Tx::CreateBucket(bolt::const_bytes name) {
    return root->CreateBucket(name);
}

std::tuple<impl::BucketPtr, bolt::ErrorCode>
Tx::CreateBucketIfNotExists(bolt::const_bytes name) {
    return root->CreateBucketIfNotExists(name);
}

bolt::ErrorCode Tx::DeleteBucket(bolt::const_bytes name) {
    return root->DeleteBucket(name);
}

bolt::ErrorCode Tx::ForEach(
    std::function<bolt::ErrorCode(bolt::const_bytes name, impl::BucketPtr b)> &&fn) {
    return root->ForEach(
        [this, fn = std::move(fn)](bolt::const_bytes k, bolt::const_bytes v) -> bolt::ErrorCode {
            return fn(k, root->RetrieveBucket(v));
        });
}

impl::BucketPtr Tx::Bucket(bolt::const_bytes name) {
    return root->RetrieveBucket(name);
}

impl::CursorPtr Tx::Cursor() { return root->Cursor(); }

}
