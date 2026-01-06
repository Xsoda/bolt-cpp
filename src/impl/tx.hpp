#ifndef __TX_HPP__
#define __TX_HPP__

#include "bucket.hpp"
#include "meta.hpp"
#include "utils.hpp"
#include <functional>
#include <memory>
#include <future>

namespace bolt::impl {

struct TxStats {
    // Page statistics.
    size_t PageCount;
    size_t PageAlloc;

    // Cursor statistics.
    size_t CursorCount;

    // Node statistics
    size_t NodeCount;
    size_t NodeDeref;

    // Rebalance statistics.
    size_t Rebalance;
    std::chrono::milliseconds RebalanceTime;

    // Split/Spill statistics.
    size_t Split;
    size_t Spill;
    std::chrono::milliseconds SpillTime;

    // Write statistics.
    size_t Write;
    std::chrono::milliseconds WriteTime;

    TxStats operator-(const TxStats &other);
    TxStats &operator+=(const TxStats &other);
};

struct Tx : public std::enable_shared_from_this<Tx> {
    bool writable;
    bool managed;
    std::weak_ptr<impl::DB> db;
    impl::meta meta;
    impl::BucketPtr root;
    std::map<impl::pgid, impl::page*> pages;
    impl::TxStats stats;
    std::vector<std::function<void()>> commitHandlers;
    int WriteFlag;

    explicit Tx();
    explicit Tx(impl::meta meta) : meta(meta){};
    explicit Tx(impl::DBPtr db, impl::meta meta);
    // init initializes the transaction.
    explicit Tx(impl::DBPtr db, bool writable);
    // ID returns the transaction id.
    impl::txid ID() const;

    // DB returns a reference to the database that created the transaction.
    std::shared_ptr<impl::DB> DB() const;

    // Size returns current database size in bytes as seen by this transaction.
    std::int64_t Size() const;

    // Writable returns whether the transaction can perform write operations.
    bool Writable() const;
    void OnCommit(std::function<void()> &&fn) {
        commitHandlers.push_back(fn);
    };

    impl::TxStats Stats() const;

    impl::page *page(impl::pgid id);

    bolt::ErrorCode writeMeta();
    bolt::ErrorCode write();

    bolt::ErrorCode Commit();
    bolt::ErrorCode Rollback();
    void rollback();
    void close();
    std::tuple<impl::page *, bolt::ErrorCode> allocate(size_t count);
    std::future<std::vector<std::string>> Check();
    void checkBucket(impl::BucketPtr bucket,
                     std::map<impl::pgid, impl::page *> &reachable,
                     std::map<impl::pgid, bool> &freed,
                     std::vector<std::string> &errors);
    void forEachPage(impl::pgid pgid, int depth,
                     std::function<void(impl::page *, int)> fn);
};

}

#endif  // !__TX_HPP__
