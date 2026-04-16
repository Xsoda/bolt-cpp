#ifndef __DB_HPP__
#define __DB_HPP__

#include "impl/batch.hpp"
#include "impl/file.hpp"
#include "impl/freelist.hpp"
#include "impl/tx.hpp"
#include "impl/utils.hpp"
#include <memory>
#include <memory_resource>
#include <mutex>
#include <shared_mutex>

namespace bolt::impl {

struct freelist;
struct meta;
struct batch;

struct DB : public std::enable_shared_from_this<DB> {
    // When enabled, the database will perform a Check() after every commit.
    // A panic is issued if the database is in an inconsistent state. This
    // flag has a large performance impact so it should only be used for
    // debugging purposes.
    bool StrictMode;
    bool NoSync;
    bool NoGrowSync;
    // MaxBatchSize is the maximum size of a batch. Default value is
    // copied from DefaultMaxBatchSize in Open.
    //
    // If <=0, disables batching.
    //
    // Do not change concurrently with calls to Batch.
    int MaxBatchSize;
    // MaxBatchDelay is the maximum delay before a batch starts.
    // Default value is copied from DefaultMaxBatchDelay in Open.
    //
    // If <=0, effectively disables batching.
    //
    // Do not change concurrently with calls to Batch.
    std::chrono::milliseconds MaxBatchDelay;

    // AllocSize is the amount of space allocated when the database
    // needs to create new pages. This is done to amortize the cost
    // of truncate() and fsync() when growing the data file.
    int AllocSize;
    int MmapFlags;

    std::string path;

    std::uintptr_t dataref;
    std::uint64_t datasz;
    std::uint64_t filesz; // current on dist file size
    impl::File file;
    impl::meta *meta0;
    impl::meta *meta1;
    std::uint32_t pageSize;
    bool opened;
    impl::TxPtr rwtx;
    std::vector<impl::TxPtr> txs;
    std::unique_ptr<impl::freelist> freelist;

    // temporary save tx page
    std::pmr::pool_options opts;
    std::pmr::synchronized_pool_resource pool;

    bolt::Stats stats;

    std::shared_ptr<impl::batch> batch;
    std::mutex batchMu;

    std::mutex rwlock;          // Allows only one writer at a time.
    std::mutex metalock;        // Protects meta page access.
    std::shared_mutex mmaplock; // Protects mmap access during remapping.
    std::shared_mutex statlock; // Protects stats access.

    // Read only mode.
    // When true, Update() and Begin(true) return ErrDatabaseReadOnly
    // immediately.
    bool readOnly;

    explicit DB();
    ~DB();
    bolt::ErrorCode init();
    const std::string &Path() const;
    bolt::ErrorCode Open(std::string path, bool readOnly = false);
    bolt::ErrorCode Close();
    void Sync();
    impl::meta *meta();

    bolt::ErrorCode colse();

    impl::page *page(impl::pgid id);
    impl::page *pageInBuffer(bolt::bytes b, impl::pgid id);
    std::tuple<impl::page *, bolt::ErrorCode> allocate(size_t count);
    bolt::ErrorCode grow(std::uint64_t sz);
    bolt::ErrorCode mmap(std::uint64_t minsz);
    bolt::ErrorCode munmap();
    void releasePage(impl::page *p);
    std::tuple<std::uint64_t, bolt::ErrorCode> mmapSize(std::uint64_t size);
    bool IsReadOnly() const { return readOnly; };

    std::tuple<impl::TxPtr, bolt::ErrorCode> Begin(bool writable);
    std::tuple<impl::TxPtr, bolt::ErrorCode> beginTx();
    std::tuple<impl::TxPtr, bolt::ErrorCode> beginRWTx();
    void removeTx(impl::TxPtr tx);
    bolt::Info Info() const;
    bolt::Stats Stats();

    bolt::ErrorCode Update(std::function<bolt::ErrorCode(impl::TxPtr)> &&fn);
    bolt::ErrorCode Batch(std::function<bolt::ErrorCode(impl::TxPtr)> &&fn);
    bolt::ErrorCode View(std::function<bolt::ErrorCode(impl::TxPtr)> &&fn);
};

} // namespace bolt::impl

#endif // !__DB_HPP__
