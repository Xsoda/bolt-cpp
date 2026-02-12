#pragma once
#ifndef BOLT_HPP
#define BOLT_HPP

#include "common.hpp"
#include "error.hpp"
#include "pimpl.hpp"
#include "stats.hpp"
#include <functional>
#include <memory>
#include <tuple>
#include <future>

namespace bolt {

namespace impl {
class DB;
class Bucket;
class Cursor;
class Tx;
using DBPtr = std::shared_ptr<DB>;
using BucketPtr = std::shared_ptr<Bucket>;
using CursorPtr = std::shared_ptr<Cursor>;
using TxPtr = std::shared_ptr<Tx>;
} // namespace impl

class Tx;
class Cursor;
class Bucket;

class DB {
public:
    bolt::ErrorCode Open(std::string path, bool readOnly = false);
    bolt::ErrorCode Close();

    bolt::ErrorCode Update(std::function<bolt::ErrorCode(Tx)> &&fn);
    bolt::ErrorCode Batch(std::function<bolt::ErrorCode(Tx)> &&fn);
    bolt::ErrorCode View(std::function<bolt::ErrorCode(Tx)> &&fn);

    std::tuple<Tx, bolt::ErrorCode> Begin(bool writable);
    bolt::Info Info();
    bolt::Stats Stats();
    bool IsReadOnly();
    std::string Path();

    DB();
    DB(bolt::impl::DBPtr db) : pImpl(db){};
    DB(const DB &) = delete;
    DB &operator=(const DB &) = delete;
    operator bool() { return pImpl; };
private:
    bolt::pimpl<bolt::impl::DBPtr> pImpl;
};

class Tx {
public:
    std::tuple<bolt::Bucket, bolt::ErrorCode>
    CreateBucket(bolt::const_bytes name);
    std::tuple<bolt::Bucket, bolt::ErrorCode>
    CreateBucketIfNotExists(bolt::const_bytes name);
    bolt::ErrorCode DeleteBucket(bolt::const_bytes name);
    bolt::ErrorCode ForEach(std::function<bolt::ErrorCode(bolt::const_bytes name,
                                                          bolt::Bucket b)> &&fn);
    bolt::Bucket Bucket(bolt::const_bytes name);
    bolt::Cursor Cursor();
    bolt::ErrorCode Commit();
    bolt::ErrorCode Rollback();

    bool Writable();
    void OnCommit(std::function<void()> &&fn);
    std::uint64_t Size();
    bolt::DB DB();
    bolt::TxStats Stats();

    std::future<std::vector<std::string>> Check();

    Tx() = delete;
    Tx(bolt::impl::TxPtr tx) : pImpl(tx){};
    Tx(const Tx &) = delete;
    Tx &operator=(const Tx &) = delete;
    operator bool() { return pImpl; };
private:
    bolt::pimpl<bolt::impl::TxPtr> pImpl;
};

class Bucket {
public:
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucket(bolt::const_bytes key);
    std::tuple<bolt::Bucket, bolt::ErrorCode>
    CreateBucketIfNotExists(bolt::const_bytes key);
    bolt::ErrorCode DeleteBucket(bolt::const_bytes key);

    bolt::const_bytes Get(bolt::const_bytes key);
    bolt::ErrorCode Put(bolt::const_bytes key, bolt::const_bytes value);
    bolt::ErrorCode Delete(bolt::const_bytes key);

    std::uint64_t Sequence();
    bolt::ErrorCode SetSequence(std::uint64_t v);
    std::tuple<std::uint64_t, bolt::ErrorCode> NextSequence();

    bolt::ErrorCode
    ForEach(std::function<bolt::ErrorCode(bolt::const_bytes key,
                                          bolt::const_bytes val)> &&fn);

    bolt::Cursor Cursor();
    bolt::Tx Tx();
    // retrieve child bucket
    bolt::Bucket RetrieveBucket(bolt::const_bytes name);

    bool Writable();
    bolt::BucketStats Stats();

    Bucket() = delete;
    Bucket(bolt::impl::BucketPtr bucket): pImpl(bucket) {};
    Bucket(const Bucket &) = delete;
    Bucket &operator=(const Bucket &) = delete;
    operator bool() { return pImpl; };
private:
    bolt::pimpl<bolt::impl::BucketPtr> pImpl;
};

class Cursor {
public:
    bolt::Bucket Bucket();
    std::tuple<bolt::const_bytes, bolt::const_bytes> First();
    std::tuple<bolt::const_bytes, bolt::const_bytes> Last();
    std::tuple<bolt::const_bytes, bolt::const_bytes> Next();
    std::tuple<bolt::const_bytes, bolt::const_bytes> Prev();

    std::tuple<bolt::const_bytes, bolt::const_bytes>
    Seek(bolt::const_bytes seek);

    bolt::ErrorCode Delete();

    Cursor() = delete;
    Cursor(bolt::impl::CursorPtr cursor) : pImpl(cursor){};
    Cursor(const Cursor &) = delete;
    Cursor &operator=(const Cursor &) = delete;
    operator bool() { return pImpl; }
private:
    bolt::pimpl<bolt::impl::CursorPtr> pImpl;
};
} // namespace bolt

#endif  // !BOLT_HPP
