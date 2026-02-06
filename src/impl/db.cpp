#include "impl/db.hpp"
#include "bolt/error.hpp"
#include "impl/async.hpp"
#include "impl/batch.hpp"
#include "impl/file.hpp"
#include "impl/freelist.hpp"
#include "impl/page.hpp"
#include "impl/tx.hpp"
#include <mutex>
#include <shared_mutex>
#include <tuple>

namespace bolt::impl {

#ifdef WIN32
bolt::ErrorCode mmap(impl::DB *db, std::uint64_t sz) {
    if (!db->readOnly) {
        auto err = db->file.Truncate(sz);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }
    auto [ptr, err] = db->file.Mmap(sz);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    db->dataref = ptr;
    db->datasz = sz;
    return bolt::ErrorCode::Success;
}
#endif

#ifdef __linux__
bolt::ErrorCode mmap(impl::DB *db, std::uint64_t sz) {
    auto [ptr, err] = db->file.Mmap(sz);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    db->dataref = ptr;
    db->datasz = sz;
    return bolt::ErrorCode::Success;
}
#endif

bolt::ErrorCode munmap(impl::DB *db) {
    if (db->dataref == (std::uintptr_t)NULL) {
        return bolt::ErrorCode::Success;
    }
    auto err = db->file.Munmap(db->dataref);
    db->dataref = (std::uintptr_t)NULL;
    db->datasz = 0;
    return err;
}

DB::DB() {
    dataref = (std::uintptr_t)NULL;
    datasz = 0;
    filesz = 0;
    meta0 = NULL;
    meta1 = NULL;
    pageSize = 0;
    opened = false;
}

DB::~DB() {
    Close();
}

bolt::ErrorCode DB::Open(std::string path, bool readOnly) {
    this->path = path;
    NoGrowSync = false;
    MmapFlags = 0;
    NoSync = false;
#ifndef NDEBUG
    StrictMode = true;
#else
    StrictMode = false;
#endif
    MaxBatchSize = bolt::DefaultMaxBatchSize;
    MaxBatchDelay = bolt::DefaultMaxBatchDelay;
    AllocSize = bolt::DefaultAllocSize;
    this->readOnly = readOnly;
    auto err = file.Open(path, readOnly);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    err = file.Flock(!readOnly, 0ms);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    std::uint64_t size;
    std::tie(size, err) = file.Size();
    if (size == 0 && err == bolt::ErrorCode::Success) {
        err = init();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    } else {
        std::vector<std::byte> buf;
        buf.assign(0x1000, std::byte(0));
        std::tie(std::ignore, err) = file.ReadAt(buf, 0);
        if (err == bolt::ErrorCode::Success) {
            auto m = pageInBuffer(buf, 0)->meta();
            err = m->validate();
            if (err != bolt::ErrorCode::Success) {
                pageSize = Getpagesize();
            } else {
                pageSize = m->pageSize;
            }
        }
    }

    // Memory map the data file.
    err = mmap(0);
    if (err != bolt::ErrorCode::Success) {
        Close();
        return err;
    }

    // Read in the freelist.
    freelist = std::make_unique<impl::freelist>();
    freelist->read(page(meta()->freelist));
    opened = true;

    return bolt::ErrorCode::Success;
}

bolt::ErrorCode DB::Close() {
    std::lock_guard<std::mutex> rw(rwlock);
    std::lock_guard<std::mutex> ml(metalock);
    std::shared_lock<std::shared_mutex> mm(mmaplock);
    if (!opened) {
        return bolt::ErrorCode::Success;
    }
    opened = false;
    freelist.reset();

    auto err = munmap();
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    // No need to unlock read-only file.
    if (!readOnly) {
        err = file.Funlock();
        if (err != bolt::ErrorCode::Success) {
        }
    }
    path = "";
    return file.Close();
}

std::string DB::Path() const {
    return path;
}

bolt::ErrorCode DB::init() {
    // Set the page size to the OS page size.
    pageSize = impl::Getpagesize();

    // Create two meta pages on a buffer.
    std::vector<std::byte> buf;
    buf.assign(pageSize * 4, (std::byte)0);
    for (int i = 0; i < 2; i++) {
        impl::page *p = pageInBuffer(buf, i);
        p->id = i;
        p->flags = impl::metaPageFlag;

        // Initialize the meta page.
        auto m = p->meta();
        m->magic = impl::magic;
        m->version = impl::version;
        m->pageSize = (std::uint32_t)pageSize;
        m->freelist = 2;
        m->root.root = 3;
        m->pgid = 4;
        m->txid = i;
        m->checksum = m->sum64();
    }

    // Write an empty freelist at page 3.
    impl::page *p = pageInBuffer(buf, (impl::pgid)2);
    p->id = 2;
    p->flags = impl::freeListPageFlag;
    p->count = 0;

    // Write an empty leaf page at page 4.
    p = pageInBuffer(buf, impl::pgid(3));
    p->id = 3;
    p->flags = impl::leafPageFlag;
    p->count = 0;

    auto [_, err] = file.WriteAt(buf, 0);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    return file.Fdatasync();
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
bolt::ErrorCode DB::Batch(std::function<bolt::ErrorCode(impl::TxPtr)> &&fn) {
    std::shared_ptr<impl::call> c = std::make_shared<impl::call>();
    do {
        std::lock_guard<std::mutex> lock(batchMu);
        if (batch == nullptr || (batch != nullptr
                                 && batch->calls.size() >= MaxBatchSize)) {
            batch = std::make_unique<impl::batch>(shared_from_this());
            AfterFunc(MaxBatchDelay, [&]() {
                batch->trigger();
            });
        }
        c->fn = std::move(fn);
        batch->calls.push_back(c);
        if (batch->calls.size() >= MaxBatchSize) {
            AsyncFireAndForget([&]() {
                batch->trigger();
            });
        }
    } while (0);

    auto f = c->err.get_future();
    f.wait();

    if (f.get() == bolt::ErrorCode::ErrorTrySolo) {
        return Update(std::move(c->fn));
    }
    return bolt::ErrorCode::Success;
}

// Update executes a function within the context of a read-write managed
// transaction. If no error is returned from the function then the transaction
// is committed. If an error is returned then the entire transaction is rolled
// back. Any error that is returned from the function or returned from the
// commit is returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a
// panic.
bolt::ErrorCode DB::Update(std::function<bolt::ErrorCode(impl::TxPtr)> &&fn) {
    auto [tx, err] = Begin(true);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    tx->managed = true;
    try {
        err = fn(tx);
    } catch (const std::exception &e) {
        fmt::println(stderr, "exception: {}", e.what());
        err = bolt::ErrorExceptionCaptured;
    }
    tx->managed = false;

    if (err != bolt::ErrorCode::Success) {
        tx->Rollback();

        if (!tx->db.expired()) {
            tx->rollback();
        }

        return err;
    }
    err = tx->Commit();

    if (!tx->db.expired()) {
        tx->rollback();
    }

    return err;
}

bolt::ErrorCode DB::View(std::function<bolt::ErrorCode(impl::TxPtr)> &&fn) {
    auto [tx, err] = Begin(false);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    tx->managed = true;
    try {
        err = fn(tx);
    } catch (const std::exception &e) {
        fmt::println(stderr, "exception: {}", e.what());
        err = bolt::ErrorExceptionCaptured;
    }
    tx->managed = false;

    if (err != bolt::ErrorCode::Success) {
        tx->Rollback();

        if (!tx->db.expired()) {
            tx->rollback();
        }

        return err;
    }
    err = tx->Rollback();

    if (!tx->db.expired()) {
        tx->rollback();
    }

    return err;
}

std::tuple<impl::TxPtr, bolt::ErrorCode> DB::Begin(bool writable) {
    if (writable) {
        return beginRWTx();
    }
    return beginTx();
}

std::tuple<impl::TxPtr, bolt::ErrorCode> DB::beginTx() {
    metalock.lock();
    mmaplock.lock_shared();
    if (!opened) {
        mmaplock.unlock_shared();
        metalock.unlock();
        return std::make_tuple<impl::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseNotOpen);

    }
    impl::TxPtr tx = std::make_shared<impl::Tx>(shared_from_this(), false);
    tx->init();
    txs.push_back(tx);
    metalock.unlock();

    statlock.lock();
    stats.TxN++;
    stats.OpenTxN = txs.size();
    statlock.unlock();
    return std::make_tuple(tx, bolt::ErrorCode::Success);
}

std::tuple<impl::TxPtr, bolt::ErrorCode> DB::beginRWTx() {
    // If the database was opened with Options.ReadOnly, return an error.
    if (readOnly) {
        return std::make_tuple<impl::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseReadOnly);
    }
    // Obtain writer lock. This is released by the transaction when it closes.
    // This enforces only one writer transaction at a time.
    rwlock.lock();

    // Once we have the writer lock then we can lock the meta pages so that
    // we can set up the transaction.
    std::lock_guard<std::mutex> _(metalock);

    // Exit if the database is not open yet.
    if (!opened) {
        rwlock.unlock();
        return std::make_tuple<impl::TxPtr, bolt::ErrorCode>(
            nullptr, bolt::ErrorCode::ErrorDatabaseNotOpen);
    }

    // Create a transaction associated with the database.
    std::shared_ptr<impl::Tx> tx =
        std::make_shared<impl::Tx>(shared_from_this(), true);
    tx->init();
    rwtx = tx;

    // Free any pages associated with closed read-only transactions.
    impl::txid minid = 0xFFFFFFFFFFFFFFFF;
    for (auto &it : txs) {
        if (it->meta.txid < minid) {
            minid = it->meta.txid;
        }
    }
    if (minid > 0) {
        freelist->release(minid - 1);
    }
    return std::make_tuple(tx, bolt::ErrorCode::Success);
}

void DB::removeTx(impl::TxPtr tx) {
    mmaplock.unlock_shared();

    metalock.lock();
    std::erase_if(txs, [&](impl::TxPtr item) -> bool { return item == tx; });
    metalock.unlock();

    statlock.lock();
    stats.OpenTxN = txs.size();
    stats.TxStats += tx->Stats();
    statlock.unlock();
}

// meta retrieves the current meta page reference.
impl::meta *DB::meta() {
    // We have to return the meta with the highest txid which doesn't fail
    // validation. Otherwise, we can cause errors when in fact the database is
    // in a consistent state. metaA is the one with the higher txid.
    auto metaA = meta0;
    auto metaB = meta1;
    if (meta1->txid > meta0->txid) {
        metaA = meta1;
        metaB = meta0;
    }

    // Use higher meta page if valid. Otherwise fallback to previous, if valid.
    if (metaA->validate() == bolt::ErrorCode::Success) {
        return metaA;
    } else if (metaB->validate() == bolt::ErrorCode::Success) {
        return metaB;
    }

    // This should never be reached, because both meta1 and meta0 were validated
    // on mmap() and we do fsync() on every write.
    _assert(false, "bolt.DB.meta(): invalid meta pages");
    return nullptr;
}

impl::page *DB::page(impl::pgid id) {
    auto pos = id * (impl::pgid)pageSize;
    return reinterpret_cast<impl::page *>(&((std::byte *)dataref)[pos]);
}

impl::page *DB::pageInBuffer(bolt::bytes b, impl::pgid id) {
    return reinterpret_cast<impl::page *>(&b[id *(impl::pgid)pageSize]);
}

// grow grows the size of the database to the given sz.
bolt::ErrorCode DB::grow(std::uint64_t sz) {
    if (sz < filesz) {
        return bolt::ErrorCode::Success;
    }

    // If the data is smaller than the alloc size then only allocate what's
    // needed. Once it goes over the allocation size then allocate in chunks.
    if (datasz < AllocSize) {
        sz = datasz;
    } else {
        sz += AllocSize;
    }

    // Truncate and fsync to ensure file size metadata is flushed.
    // https://github.com/boltdb/bolt/issues/284
    if (!NoGrowSync && !readOnly) {
        auto err = file.Truncate(sz);
        if (err != bolt::ErrorCode::Success) {
            return bolt::ErrorCode::ErrorFileResizeFail;
        }
        err = file.Fsync();
        if (err != bolt::ErrorCode::Success) {
            return bolt::ErrorCode::ErrorFileSyncFail;
        }
    }
    filesz = sz;
    return bolt::ErrorCode::Success;
}

// allocate returns a contiguous block of memory starting at a given page.
std::tuple<impl::page *, bolt::ErrorCode> DB::allocate(size_t count) {
    // Allocate a temporary buffer for the page.
    std::lock_guard<std::mutex> lock(poolMutex);
    auto buf = std::make_unique<std::vector<std::byte>>();
    buf->assign(count * pageSize, std::byte(0));
    impl::page *p = reinterpret_cast<impl::page *>(&(*buf)[0]);
    p->overflow = std::uint32_t(count - 1);

    // Use pages from the freelist if they are available.
    p->id = freelist->allocate(count);
    if (p->id != 0) {
        pagePool.insert(std::make_pair(p, std::move(buf)));
        return std::make_tuple(p, bolt::ErrorCode::Success);
    }

    // Resize mmap() if we're at the end.
    p->id = rwtx->meta.pgid;
    auto minsz = (p->id + count + 1) * pageSize;
    if (minsz >= datasz) {
        auto err = mmap(minsz);
        if (err != bolt::ErrorCode::Success) {
            return std::make_tuple(nullptr, err);
        }
    }

    // Move the page id high water mark.
    rwtx->meta.pgid += impl::pgid(count);
    pagePool.insert(std::make_pair(p, std::move(buf)));
    return std::make_tuple(p, bolt::ErrorCode::Success);
}

void DB::releasePage(impl::page *p) {
    std::lock_guard<std::mutex> lock(poolMutex);
    auto it = pagePool.find(p);
    if (it != pagePool.end()) {
        pagePool.erase(it);
    }
}

bolt::ErrorCode DB::mmap(std::uint64_t minsz) {
    std::unique_lock<std::shared_mutex> lock(mmaplock);
    auto [size, err] = file.Size();
    if (err != bolt::ErrorCode::Success) {
        return err;
    } else if (size < pageSize * 2) {
        return bolt::ErrorCode::ErrorFileSizeTooSmall;
    }

    // Ensure the size is at least the minimum size.
    if (size < std::uint64_t(minsz)) {
        size = std::uint64_t(minsz);
    }

    std::tie(size, err) = mmapSize(size);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    // Dereference all mmap references before unmapping.
    if (rwtx != nullptr) {
        rwtx->root->dereference();
    }

    // Unmap existing data before continuing.
    err = munmap();
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    // Memory-map the data file as a byte slice.
    err = impl::mmap(this, size);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    // Save references to the meta pages.
    meta0 = page(0)->meta();
    meta1 = page(1)->meta();

    // Validate the meta pages. We only return an error if both meta pages fail
    // validation, since meta0 failing validation means that it wasn't saved
    // properly -- but we can recover using meta1. And vice-versa.
    auto err0 = meta0->validate();
    auto err1 = meta1->validate();
    if (err0 != bolt::ErrorCode::Success && err1 != bolt::ErrorCode::Success) {
        return err0;
    }
    return bolt::ErrorCode::Success;
}

// munmap unmaps the data file from memory.
bolt::ErrorCode DB::munmap() {
    auto err = impl::munmap(this);
    if (err != bolt::ErrorCode::Success) {
        return err;
    }
    return bolt::ErrorCode::Success;
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
std::tuple<std::uint64_t, bolt::ErrorCode> DB::mmapSize(std::uint64_t size) {
    // Double the size from 32KB until 1GB.
    for (std::uint32_t i = 15; i <= 30; i++) {
        if (size <= std::uint64_t(1) << i) {
            return std::make_tuple(1 << i, bolt::ErrorCode::Success);
        }
    }

    // Verify the requested size is not above the maximum allowed.
    if (size > impl::maxMapSize) {
        return std::make_tuple(0, bolt::ErrorCode::ErrorMmapTooLarge);
    }

    // Ensure that the mmap size is a multiple of the page size.
    // This should always be true since we're incrementing in MBs.
    if (size % std::uint64_t(pageSize) != 0) {
        size = ((size / pageSize) + 1) * pageSize;
    }

    // If we've exceeded the max size then only grow up to the max size.
    if (size > impl::maxMapSize) {
        size = impl::maxMapSize;
    }
    return std::make_tuple(size, bolt::ErrorCode::Success);
}

impl::Info DB::Info() const {
    impl::Info info;
    info.Data = dataref;
    info.PageSize = pageSize;
    return info;
}

impl::Stats DB::Stats() {
    std::shared_lock<std::shared_mutex> lock(statlock);
    return stats;
}

}
