#include "tx.hpp"
#include "page.hpp"
#include "bucket.hpp"
#include "db.hpp"
#include "meta.hpp"
#include "freelist.hpp"
#include "utils.hpp"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <mutex>
#include <inttypes.h>

namespace bolt::impl {

TxStats TxStats::operator-(const TxStats &other) {
    TxStats diff;
    diff.PageCount = PageCount - other.PageCount;
    diff.PageAlloc = PageAlloc - other.PageAlloc;
    diff.CursorCount = CursorCount - other.CursorCount;
    diff.NodeCount = NodeCount - other.NodeCount;
    diff.NodeDeref = NodeDeref - other.NodeDeref;
    diff.Rebalance = Rebalance - other.Rebalance;
    diff.RebalanceTime = RebalanceTime - other.RebalanceTime;
    diff.Split = Split - other.Split;
    diff.Spill = Spill - other.Spill;
    diff.SpillTime = SpillTime - other.SpillTime;
    diff.Write = Write - other.Write;
    diff.WriteTime = WriteTime - other.WriteTime;
    return diff;
}

TxStats &TxStats::operator+=(const TxStats &other) {
    PageCount += other.PageCount;
    PageAlloc += other.PageAlloc;
    CursorCount += other.CursorCount;
    NodeCount += other.NodeCount;
    NodeDeref += other.NodeDeref;
    Rebalance += other.Rebalance;
    RebalanceTime += other.RebalanceTime;
    Split += other.Split;
    Spill += other.Spill;
    SpillTime += other.SpillTime;
    Write += other.Write;
    WriteTime += other.WriteTime;
    return *this;
}

Tx::Tx(impl::DBPtr db, impl::meta meta) : db(db), meta(meta) {}

Tx::Tx() {}

Tx::Tx(std::shared_ptr<impl::DB> db, bool writable)
    : db(db), writable(writable) {
    db->meta()->copy(&meta);
    root->bucket = meta.root;
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
    assert("Tx already closed" && false);
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
    assert("Tx already closed" && false);
    return nullptr;
}

bolt::ErrorCode Tx::writeMeta() {
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    }
    std::vector<std::byte> buf;
    buf.assign(dbptr->pageSize, std::byte(0x00));

    impl::page *p = dbptr->pageInBuffer(bolt::bytes(buf.begin(), buf.end()), 0);
    meta.write(p);
    auto [_, err] = dbptr->file.WriteAt(bolt::bytes(buf.begin(), buf.end()),
                                (std::int64_t)p->id * dbptr->pageSize);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    if (!dbptr->NoSync) {
        err = dbptr->file.Fdatasync();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }
    stats.Write++;
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode Tx::write() {
    // Sort pages by id.
    std::vector<impl::page *> pages;
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorCode::ErrorTxClosed;
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
            if (err != bolt::ErrorCode::Success) {
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
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }
    // Put small pages back to page pool.
    for (auto it : pages) {
        dbptr->releasePage(it);
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode Tx::Commit() {
    assert("managed tx commit not allowed" && !managed);
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!writable) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    }
    auto startTime = std::chrono::system_clock::now();
    auto since = [](std::chrono::time_point<std::chrono::system_clock> start) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - start);
    };
    root->rebalance();
    if (stats.Rebalance > 0) {
        stats.RebalanceTime += since(startTime);
    }

    startTime = std::chrono::system_clock::now();

    auto err = root->spill();
    if (err != bolt::ErrorCode::Success) {
        rollback();
        return err;
    }
    stats.SpillTime += since(startTime);

    meta.root.root = root->bucket.root;

    auto opgid = meta.pgid;
    dbptr->freelist->free(meta.txid, dbptr->page(meta.freelist));
    auto [p, ret] = allocate((dbptr->freelist->size() / dbptr->pageSize) + 1);
    if (ret != bolt::ErrorCode::Success) {
        rollback();
        return ret;
    }
    err = dbptr->freelist->write(p);
    if (err != bolt::ErrorCode::Success) {
        rollback();
        return err;
    }

    meta.freelist = p->id;
    if (meta.pgid > opgid) {
        err = dbptr->grow((meta.pgid + 1) * dbptr->pageSize);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }

    startTime = std::chrono::system_clock::now();
    err = write();
    if (err != bolt::ErrorCode::Success) {
        rollback();
        return err;
    }

    // If strict mode is enabled then perform a consistency check.
    // Only the first consistency error is reported in the panic.
    if (dbptr->StrictMode) {
        auto result = Check();
        auto errors = result.get();
        assert("check fail" && errors.empty());
    }

    err = writeMeta();
    if (err != bolt::ErrorCode::Success) {
        rollback();
        return err;
    }
    stats.WriteTime += since(startTime);
    close();
    for (auto &fn : commitHandlers) {
        fn();
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode Tx::Rollback() {
    assert("managed tx rollback not allowed" && !managed);
    auto dbptr = db.lock();
    if (!dbptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    }
    rollback();
    return bolt::ErrorCode::Success;
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

    dbptr.reset();
    pages.clear();
}

// allocate returns a contiguous block of memory starting at a given page.
std::tuple<impl::page *, bolt::ErrorCode> Tx::allocate(size_t count) {
    auto dbptr = db.lock();
    if (!dbptr) {
        return std::make_tuple(nullptr, bolt::ErrorCode::ErrorTxClosed);
    }
    auto [p, err] = dbptr->allocate(count);
    pages[p->id] = p;
    stats.PageCount++;
    stats.PageAlloc += count * dbptr->pageSize;
    return std::make_tuple(p, bolt::ErrorCode::Success);
}

std::future<std::vector<std::string>> Tx::Check() {
    auto fn = [this]() -> std::vector<std::string> {
      std::map<impl::pgid, bool> freed;
      std::vector<impl::pgid> all;
      std::vector<std::string> errors;
      char buf[1024];
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
          snprintf(buf, sizeof(buf), "page %" PRIu64 ": already freed", item);
          errors.push_back(buf);
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
          snprintf(buf, sizeof(buf), "page %" PRIu64 ": unreachable unfreed", i);
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
    if (bucket->bucket.root == 0) {
        return;
    }
    auto txptr = bucket->tx.lock();
    if (!txptr) {
        return;
    }

    // Check every page used by this bucket.
    txptr->forEachPage(bucket->bucket.root, 0, [&](impl::page *p, int depth) {
        char buf[1024];
        if (p->id > txptr->meta.pgid) {
            snprintf(buf, sizeof(buf), "page %" PRIu64 ": out of bounds: %" PRIu64, p->id,
                     txptr->meta.pgid);
            errors.push_back(buf);
        }
        // Ensure each page is only referenced once.
        for (impl::pgid i = 0; i <= p->overflow; i++) {
            auto id = p->id + 1;
            auto it = reachable.find(id);
            if (it != reachable.end()) {
                snprintf(buf, sizeof(buf), "page %" PRIu64 ": multiple references", id);
                errors.push_back(buf);
            }
            reachable[id] = p;
        }

        // We should only encounter un-freed leaf and branch pages.
        auto it = freed.find(p->id);
        if (it != freed.end()) {
            snprintf(buf, sizeof(buf), "page %" PRIu64 ": reachable freed", p->id);
            errors.push_back(buf);
        } else if ((p->flags & impl::branchPageFlag) == 0 &&
                   (p->flags & impl::leafPageFlag) == 0) {
            snprintf(buf, sizeof(buf), "page %" PRIu64 ": invalid type: %s", p->id,
                     p->type().c_str());
            errors.push_back(buf);
        }
    });

    // Check each bucket within this bucket.
    bucket->ForEach([&](bolt::bytes key, bolt::bytes val) -> bolt::ErrorCode {
        return bolt::ErrorCode::Success;
    });
}

void Tx::forEachPage(impl::pgid pgid, int depth,
                     std::function<void(impl::page *, int)> fn) {
    auto p = page(pgid);

    // Execute function.
    fn(p, depth);

    // Recursively loop over children.
    if (p->flags & impl::branchPageFlag) {
        for (std::uint16_t i = 0; i < p->count; i++) {
            auto elem = p->branchPageElement(i);
            forEachPage(elem->pgid, depth + 1, fn);
        }
    }
}

}
