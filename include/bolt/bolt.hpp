#pragma once
#ifndef BOLT_HPP
#define BOLT_HPP

#include "common.hpp"
#include "error.hpp"
#include "pimpl.hpp"
#include "stats.hpp"
#include <functional>
#include <future>
#include <memory>
#include <tuple>

namespace bolt {

namespace impl {
struct DB;
struct Bucket;
struct Cursor;
struct Tx;
using DBPtr = std::shared_ptr<DB>;
using BucketPtr = std::shared_ptr<Bucket>;
using CursorPtr = std::shared_ptr<Cursor>;
using TxPtr = std::shared_ptr<Tx>;
} // namespace impl

class Tx;
class Cursor;
class Bucket;

class DB final : private pimpl<impl::DBPtr> {
public:
    bolt::ErrorCode Open(std::string path, bool readOnly = false);
    bolt::ErrorCode Close();

    bolt::ErrorCode Update(std::function<bolt::ErrorCode(bolt::Tx)> &&fn);
    bolt::ErrorCode Batch(std::function<bolt::ErrorCode(bolt::Tx)> &&fn);
    bolt::ErrorCode View(std::function<bolt::ErrorCode(bolt::Tx)> &&fn);

    std::tuple<bolt::Tx, bolt::ErrorCode> Begin(bool writable);
    bolt::Info Info();
    bolt::Stats Stats();
    bool IsReadOnly();
    std::string Path();

    DB();
    DB(bolt::impl::DBPtr db) : pimpl(db){};
    DB(const bolt::DB &) = delete;
    bolt::DB &operator=(const bolt::DB &) = delete;
    DB(bolt::DB &&) = default;
    bolt::DB &operator=(bolt::DB &&) = default;
    ~DB() = default;
    operator bool() { return pimpl<impl::DBPtr>::impl() ? true : false; };
};

class Tx final : pimpl<impl::TxPtr> {
public:
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucket(bolt::const_bytes name);
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucketIfNotExists(bolt::const_bytes name);
    bolt::ErrorCode DeleteBucket(bolt::const_bytes name);

    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucket(const std::string &name);
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucketIfNotExists(const std::string &name);
    bolt::ErrorCode DeleteBucket(const std::string &name);

    // <bucket>/<bucket>/.../<last-bucket>
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucketWithPath(const std::string &path);
    std::tuple<bolt::Bucket, bolt::ErrorCode> RetrieveBucketWithPath(const std::string &path);

    bolt::ErrorCode
    ForEach(std::function<bolt::ErrorCode(bolt::const_bytes name, bolt::Bucket b)> &&fn);
    bolt::Bucket Bucket(bolt::const_bytes name);
    bolt::Bucket Bucket(const std::string &name);

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
    Tx(bolt::impl::TxPtr tx) : pimpl(tx){};
    Tx(const Tx &) = delete;
    bolt::Tx &operator=(const Tx &) = delete;
    Tx(bolt::Tx &&) = default;
    bolt::Tx &operator=(bolt::Tx &&) = default;
    ~Tx() = default;
    operator bool() { return pimpl<impl::TxPtr>::impl() ? true : false; };
};

class Bucket final : private pimpl<impl::BucketPtr> {
public:
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucket(bolt::const_bytes key);
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucketIfNotExists(bolt::const_bytes key);
    bolt::ErrorCode DeleteBucket(bolt::const_bytes key);

    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucket(const std::string &key);
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucketIfNotExists(const std::string &key);
    bolt::ErrorCode DeleteBucket(const std::string &key);

    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucket(const char *key, int klen);
    std::tuple<bolt::Bucket, bolt::ErrorCode> CreateBucketIfNotExists(const char *key, int klen);
    bolt::ErrorCode DeleteBucket(const char *key, int klen);

    bolt::const_bytes Get(bolt::const_bytes key);
    bolt::ErrorCode Put(bolt::const_bytes key, bolt::const_bytes value);
    bolt::ErrorCode Delete(bolt::const_bytes key);

    bolt::const_bytes Get(const std::string &key);
    bolt::ErrorCode Put(const std::string &key, const std::string &value);
    bolt::ErrorCode Delete(const std::string &key);

    bolt::const_bytes Get(const char *key, int klen);
    bolt::ErrorCode Put(const char *key, int klen, const char *value, int vlen);
    bolt::ErrorCode Delete(const char *key, int klen);

    std::uint64_t Sequence();
    bolt::ErrorCode SetSequence(std::uint64_t v);
    std::tuple<std::uint64_t, bolt::ErrorCode> NextSequence();

    float GetFillPercent();
    void SetFillPercent(float fill);

    bolt::ErrorCode
    ForEach(std::function<bolt::ErrorCode(bolt::const_bytes key, bolt::const_bytes val)> &&fn);

    bolt::Cursor Cursor();
    bolt::Tx Tx();
    // retrieve child bucket
    bolt::Bucket RetrieveBucket(bolt::const_bytes name);

    bool Writable();
    bolt::BucketStats Stats();

    Bucket() = delete;
    Bucket(bolt::impl::BucketPtr bucket) : pimpl(bucket){};
    Bucket(const bolt::Bucket &) = delete;
    bolt::Bucket &operator=(const bolt::Bucket &) = delete;
    Bucket(bolt::Bucket &&) = default;
    bolt::Bucket &operator=(bolt::Bucket &&) = default;
    ~Bucket() = default;
    operator bool() { return pimpl<impl::BucketPtr>::impl() ? true : false; };
};

class Cursor final : private pimpl<impl::CursorPtr> {
public:
    bolt::Bucket Bucket();
    std::tuple<bolt::const_bytes, bolt::const_bytes> First();
    std::tuple<bolt::const_bytes, bolt::const_bytes> Last();
    std::tuple<bolt::const_bytes, bolt::const_bytes> Next();
    std::tuple<bolt::const_bytes, bolt::const_bytes> Prev();

    std::tuple<bolt::const_bytes, bolt::const_bytes> Seek(bolt::const_bytes seek);

    bolt::ErrorCode Delete();

    Cursor() = delete;
    Cursor(bolt::impl::CursorPtr cursor) : pimpl(cursor){};
    Cursor(const Cursor &) = delete;
    bolt::Cursor &operator=(const bolt::Cursor &) = delete;
    Cursor(bolt::Cursor &&) = default;
    bolt::Cursor &operator=(bolt::Cursor &&) = default;
    ~Cursor() = default;
    operator bool() { return pimpl<impl::CursorPtr>::impl() ? true : false; };
};

const char *library_version();

} // namespace bolt

#endif // !BOLT_HPP
