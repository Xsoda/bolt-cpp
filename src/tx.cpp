#include "tx.hpp"
#include "common.hpp"
#include "page.hpp"
#include "bucket.hpp"
#include "db.hpp"
#include "meta.hpp"
#include "freelist.hpp"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <mutex>

namespace bolt {

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

Tx::Tx(bolt::DB *db, bool writable): root(bolt::Bucket(shared_from_this())), db(db), writable(writable) {
    db->meta()->copy(&meta);
    *root.bucket = meta.root;
    if (writable) {
        meta.txid += 1;
    }
}

bolt::DB *Tx::DB() const { return db; }

int Tx::ID() const { return meta.txid; }

std::int64_t Tx::Size() const { return meta.pgid * db->pageSize; }

bool Tx::Writable() const { return writable; }

bolt::TxStats Tx::Stats() const { return stats; }

bolt::page *Tx::page(bolt::pgid id) {
    auto it = pages.find(id);
    if (it != pages.end()) {
        return it->second;
    }
    return db->page(id);
}

bolt::ErrorCode Tx::writeMeta() {
    std::vector<std::byte> buf;
    buf.assign(db->pageSize, std::byte(0x00));

    bolt::page *p = db->pageInBuffer(bolt::bytes(buf.begin(), buf.end()), 0);
    meta.write(p);
    auto [_, err] = db->file.WriteAt(bolt::bytes(buf.begin(), buf.end()),
                                (std::int64_t)p->id * db->pageSize);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    if (!db->NoSync) {
        err = db->file.Fdatasync();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }
    stats.Write++;
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode Tx::write() {
    std::vector<bolt::page *> pages;
    pages.reserve(this->pages.size());
    for (auto [key, value] : this->pages) {
        pages.push_back(value);
    }
    this->pages.clear();

    std::sort(pages.begin(), pages.end(),
              [&](bolt::page *a, bolt::page *b) { return a->id < b->id; });

    for (auto it : pages) {
        auto size = int(it->overflow + 1) * db->pageSize;
        auto offset = std::int64_t(it->id) * db->pageSize;

        auto ptr = reinterpret_cast<std::byte *>(it);
        while (true) {
            auto sz = size;
            if (sz > bolt::maxAllocSize - 1) {
                sz = bolt::maxAllocSize - 1;
            }
            auto [_, err] = db->file.WriteAt(bolt::bytes(ptr, sz), offset);
            if (err != bolt::ErrorCode::Success) {
                return err;
            }
            stats.Write++;
            size -= sz;
            if (size == 0) {
                break;
            }
            offset += sz;
            ptr = &ptr[sz];
        }
    }

    if (!db->NoSync) {
        auto err = db->file.Fdatasync();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode Tx::Commit() {
    assert("managed tx commit not allowed" && !managed);
    if (db == nullptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!writable) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    }
    auto startTime = std::chrono::system_clock::now();
    auto since = [](std::chrono::time_point<std::chrono::system_clock> start) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - start);
    };
    root.rebalance();
    if (stats.Rebalance > 0) {
        stats.RebalanceTime += since(startTime);
    }

    startTime = std::chrono::system_clock::now();

    auto err = root.spill();
    if (err != bolt::ErrorCode::Success) {
        rollback();
        return err;
    }
    stats.SpillTime += since(startTime);

    meta.root.root = root.bucket->root;

    auto opgid = meta.pgid;
    db->freelist->free(meta.txid, db->page(meta.freelist));
    auto [p, ret] = allocate((db->freelist->size() / db->pageSize) + 1);
    if (ret != bolt::ErrorCode::Success) {
        rollback();
        return ret;
    }
    err = db->freelist->write(p);
    if (err != bolt::ErrorCode::Success) {
        rollback();
        return err;
    }

    meta.freelist = p->id;
    if (meta.pgid > opgid) {
        err = db->grow((meta.pgid + 1) * db->pageSize);
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

    if (db->StrictMode) {
        // TODO
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
    if (db == nullptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    }
    rollback();
    return bolt::ErrorCode::Success;
}

void Tx::rollback() {
    if (db == nullptr) {
        return;
    }
    if (writable) {
        db->freelist->rollback(meta.txid);
        db->freelist->reload(db->page(db->meta()->freelist));
    }
    close();
}

void Tx::close() {
    if (db == nullptr) {
        return;
    }
    if (writable) {
        int freelistFreeN = db->freelist->free_count();
        int freelistPendingN = db->freelist->pending_count();
        int freelistAlloc = db->freelist->size();

        db->rwtx = nullptr;
        db->rwlock.unlock();

        std::unique_lock<std::shared_mutex> lock(db->statlock);
        db->stats.FreePageN = freelistFreeN;
        db->stats.PendingPageN = freelistPendingN;
        db->stats.FreeAlloc = (freelistFreeN + freelistPendingN) * db->pageSize;
        db->stats.FreelistInuse = freelistAlloc;
        db->stats.TxStats += stats;
    } else {
        db->removeTx(shared_from_this());
    }

    db = nullptr;
    pages.clear();
}

}
