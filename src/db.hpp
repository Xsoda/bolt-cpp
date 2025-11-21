#ifndef __DB_HPP__
#define __DB_HPP__

#include "common.hpp"
#include "tx.hpp"
#include "file.hpp"
#include <mutex>
#include <shared_mutex>

namespace bolt {

struct freelist;
struct meta;
struct Tx;
struct batch;
struct Stats {
    int FreePageN;
    int PendingPageN;
    int FreeAlloc;
    int FreelistInuse;
    int TxN;
    int OpenTxN;
    bolt::TxStats TxStats;
};

struct DB {
    bool StrictMode;
    bool NoSync;
    bool NoGrowSync;
    int MaxBatchSize;
    std::chrono::milliseconds MaxBatchDelay;
    int AllocSize;
    int MmapFlags;

    std::string path;

    std::uintptr_t dataref;
    int datasz;
    int filesz;
    bolt::File file;
    bolt::meta *meta0;
    bolt::meta *meta1;
    int pageSize;
    bool opened;
    bolt::Tx *rwtx;
    std::vector<bolt::Tx*> txs;
    bolt::freelist *freelist;
    bolt::Stats stats;

    std::unique_ptr<bolt::batch> batch;
    std::mutex batchMu;

    std::mutex rwlock;
    std::mutex metalock;
    std::shared_mutex mmaplock;
    std::shared_mutex statlock;

    bool readOnly;

    bolt::ErrorCode init();
    std::string Path() const;
    bolt::ErrorCode Open(std::string path);
    bolt::ErrorCode Close();
    bolt::Tx *Begin(bool writable);
    void Sync();
    bolt::meta *meta();

    bolt::page *page(bolt::pgid id);

    bolt::page *pageInBuffer(bolt::bytes b, bolt::pgid id);

    bolt::ErrorCode Update(std::function<bolt::ErrorCode(bolt::Tx*)> &&fn);
    bolt::ErrorCode Batch(std::function<bolt::ErrorCode(bolt::Tx*)> &&fn);
};

}

#endif  // !__DB_HPP__
