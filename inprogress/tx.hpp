#ifndef __TX_HPP__
#define __TX_HPP__

#include "common.hpp"
#include "db.hpp"
#include "bucket.hpp"
#include "meta.hpp"
#include <memory>
#include <functional>

namespace bolt {

class TxStats {

};

struct Tx {
    bool writable;
    bool managed;
    bolt::DB *db;
    bolt::meta *meta;
    bolt::Bucket root;
    std::map<bolt::pgid, bolt::page*> pages;
    bolt::TxStats stats;
    std::vector<std::function<void()>> commitHandlers;
    int WriteFlag;

    // init initializes the transaction.
    void init(bolt::DB *db);
    // ID returns the transaction id.
    int ID() const;

    // DB returns a reference to the database that created the transaction.
    bolt::DB *DB();

    // Size returns current database size in bytes as seen by this transaction.
    std::int64_t Size() const;

    // Writable returns whether the transaction can perform write operations.
    bool Writable() const;
};

}

#endif  // !__TX_HPP__
