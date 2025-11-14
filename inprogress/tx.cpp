#include "tx.hpp"
#include "bucket.hpp"
#include "db.hpp"
#include "meta.hpp"

namespace bolt {

class Tx::Impl {
    bool writable;
    bool managed;
    bolt::DB *db;
    bolt::meta *meta;
    bolt::Bucket root;
    std::map<bolt::pgid, bolt::page*> pages;
    bolt::TxStats stats;
    int WriteFlag;

    int ID() const { return (int)meta->txid; };
    bolt::DB *DB() const { return db; };
    std::int64_t Size() const { return (std::int64_t)meta->pgid * (std::int64_t)db->PageSize(); };
};

}
