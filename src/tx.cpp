#include "tx.hpp"
#include "common.hpp"
#include "page.hpp"
#include "bucket.hpp"
#include "db.hpp"
#include "meta.hpp"
#include <algorithm>

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

Tx::Tx(bolt::DB *db, bool writable): root(bolt::Bucket(this)), db(db), writable(writable) {
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
    auto err = db->file.writeAt(bolt::bytes(buf.begin(), buf.end()),
                                (std::int64_t)p->id * db->pageSize);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    if (!db->NoSync) {
        err = db->file.fdatasync();
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
            auto err = db->file.writeAt(bolt::bytes(ptr, sz), offset);
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
        auto err = db->file.fdatasync();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }
    return bolt::ErrorCode::Success;
}


}
