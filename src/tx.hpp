#ifndef __TX_HPP__
#define __TX_HPP__

#include "bucket.hpp"
#include "common.hpp"
#include "error.hpp"
#include "meta.hpp"
#include <functional>
#include <memory>

namespace bolt {

struct DB;

struct TxStats {
    // Page statistics.
    int PageCount;
    int PageAlloc;

    // Cursor statistics.
    int CursorCount;

    // Node statistics
    int NodeCount;
    int NodeDeref;

    // Rebalance statistics.
    int Rebalance;
    std::chrono::milliseconds RebalanceTime;

    // Split/Spill statistics.
    int Split;
    int Spill;
    std::chrono::milliseconds SpillTime;

    // Write statistics.
    int Write;
    std::chrono::milliseconds WriteTime;

    TxStats operator-(const TxStats &other);
    TxStats &operator+=(const TxStats &other);
};

struct Tx : public std::enable_shared_from_this<Tx> {
    bool writable;
    bool managed;
    std::weak_ptr<bolt::DB> db;
    bolt::meta meta;
    bolt::BucketPtr root;
    std::map<bolt::pgid, bolt::page*> pages;
    bolt::TxStats stats;
    std::vector<std::function<void()>> commitHandlers;
    int WriteFlag;

    explicit Tx();
    explicit Tx(bolt::meta meta) : meta(meta) {};
    // init initializes the transaction.
    explicit Tx(std::shared_ptr<bolt::DB> db, bool writable);
    // ID returns the transaction id.
    int ID() const;

    // DB returns a reference to the database that created the transaction.
    std::shared_ptr<bolt::DB> DB() const;

    // Size returns current database size in bytes as seen by this transaction.
    std::int64_t Size() const;

    // Writable returns whether the transaction can perform write operations.
    bool Writable() const;
    void OnCommit(std::function<void()> &&fn) {
        commitHandlers.push_back(fn);
    };

    bolt::TxStats Stats() const;

    bolt::page *page(bolt::pgid id);

    bolt::ErrorCode writeMeta();
    bolt::ErrorCode write();

    bolt::ErrorCode Commit();
    bolt::ErrorCode Rollback();
    void rollback();
    void close();
    std::tuple<bolt::page*, bolt::ErrorCode> allocate(int count);
};

}

#endif  // !__TX_HPP__
