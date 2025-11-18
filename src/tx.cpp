#include "tx.hpp"
#include "bucket.hpp"
#include "db.hpp"
#include "meta.hpp"

namespace bolt {

TxStats &TxStats::operator-(const TxStats &other) {
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

Tx::Tx(bolt::DB *db): root(bolt::Bucket(this)), db(db) {
    db->meta()->copy(&meta);
    root.bucket = meta.root;
    if (writable) {
        meta.txid += 1;
    }
}

}
