#ifndef __DB_HPP__
#define __DB_HPP__

#include "common.hpp"
#include "freelist.hpp"
#include "tx.hpp"
#include <mutex>
#include <shared_mutex>

namespace bolt {

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
    bolt::meta *meta0;
    bolt::meta *meta1;
    int pageSize;
    bool opene;
    bolt::Tx *rwtx;
    std::vector<bolt::Tx*> txs;
    bolt::freelist *freelist;
    bolt::Stats stats;

    std::mutex rwlock;
    std::mutex metalock;
    std::shared_mutex mmaplock;
    std::shared_mutex statlock;

    bool readOnly;

    std::string Path() const;
    int Open(std::string path);
    int Close();
    bolt::Tx *Begin(bool writable);
    void Sync();
    bolt::meta *meta();
};

}

#endif  // !__DB_HPP__
