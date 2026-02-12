#include "bolt/bolt.hpp"
#include "impl/bucket.hpp"
#include "impl/db.hpp"
#include "impl/tx.hpp"
#include "impl/cursor.hpp"

namespace bolt {

// DB
DB::DB() : pImpl(std::make_shared<impl::DB>()) {}

bolt::ErrorCode DB::Open(std::string path, bool readOnly) {
    return pImpl->get()->Open(path, readOnly);
}

bolt::ErrorCode DB::Close() {
    return pImpl->get()->Close();
}

bolt::ErrorCode DB::Update(std::function<bolt::ErrorCode(bolt::Tx)> &&fn) {
    return pImpl->get()->Update(
        [fn = std::move(fn)](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            return fn(tx);
        });
}

bolt::ErrorCode DB::Batch(std::function<bolt::ErrorCode(bolt::Tx)> &&fn) {
    return pImpl->get()->Batch(
        [fn = std::move(fn)](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            return fn(tx);
        });
}

bolt::ErrorCode DB::View(std::function<bolt::ErrorCode(bolt::Tx)> &&fn) {
    return pImpl->get()->View(
        [fn = std::move(fn)](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            return fn(tx);
    });
}

std::tuple<Tx, bolt::ErrorCode> DB::Begin(bool writable) {
    auto [tx, err] = pImpl->get()->Begin(writable);
    return std::make_pair(tx, err);
}

// Tx
std::tuple<bolt::Bucket, bolt::ErrorCode>
Tx::CreateBucket(bolt::const_bytes name) {
    auto [b, err] = pImpl->get()->CreateBucket(name);
    return std::make_pair(b, err);
}

std::tuple<bolt::Bucket, bolt::ErrorCode>
Tx::CreateBucketIfNotExists(bolt::const_bytes name) {
    auto [b, err] = pImpl->get()->CreateBucketIfNotExists(name);
    return std::make_pair(b, err);
}

bolt::ErrorCode Tx::DeleteBucket(bolt::const_bytes name) {
    return pImpl->get()->DeleteBucket(name);
}

bolt::ErrorCode Tx::ForEach(
    std::function<bolt::ErrorCode(bolt::const_bytes name, bolt::Bucket b)>
        &&fn) {
    return pImpl->get()->ForEach(fn);
}

bolt::Bucket Tx::Bucket(bolt::const_bytes name) {
    return pImpl->get()->Bucket(name);
}

bolt::Cursor Tx::Cursor() { return pImpl->get()->Cursor(); }

bolt::ErrorCode Tx::Commit() { return pImpl->get()->Commit(); }

bolt::ErrorCode Tx::Rollback() { return pImpl->get()->Rollback(); }

std::future<std::vector<std::string>> Tx::Check() {
    return pImpl->get()->Check();
}

// Bucket
std::tuple<bolt::Bucket, bolt::ErrorCode>
Bucket::CreateBucket(bolt::const_bytes key) {
    auto [b, err] = pImpl->get()->CreateBucket(key);
    return std::make_tuple(b, err);
}

std::tuple<bolt::Bucket, bolt::ErrorCode>
Bucket::CreateBucketIfNotExists(bolt::const_bytes key) {
    auto [b, err] = pImpl->get()->CreateBucketIfNotExists(key);
    return std::make_tuple(b, err);
}

bolt::ErrorCode Bucket::DeleteBucket(bolt::const_bytes key) {
    return pImpl->get()->DeleteBucket(key);
}

bolt::const_bytes Bucket::Get(bolt::const_bytes key) {
    return pImpl->get()->Get(key);
}

bolt::ErrorCode Bucket::Put(bolt::const_bytes key, bolt::const_bytes value) {
    return pImpl->get()->Put(key, value);
}

bolt::ErrorCode Bucket::Delete(bolt::const_bytes key) {
    return pImpl->get()->Delete(key);
}

std::uint64_t Bucket::Sequence() { return pImpl->get()->Sequence(); }

bolt::ErrorCode Bucket::SetSequence(std::uint64_t v) {
    return pImpl->get()->SetSequence(v);
}

std::tuple<std::uint64_t, bolt::ErrorCode> Bucket::NextSequence() {
    return pImpl->get()->NextSequence();
}

bolt::ErrorCode Bucket::ForEach(
    std::function<bolt::ErrorCode(bolt::const_bytes key, bolt::const_bytes val)>
        &&fn) {
    return pImpl->get()->ForEach(std::move(fn));
}

bolt::Cursor Bucket::Cursor() { return pImpl->get()->Cursor(); }

bolt::Tx Bucket::Tx() { return pImpl->get()->Tx(); }

bolt::Bucket Bucket::RetrieveBucket(bolt::const_bytes name) {
    return pImpl->get()->RetrieveBucket(name);
}

// Cursor
bolt::Bucket Cursor::Bucket() { return pImpl->get()->Bucket(); }

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::First() {
    return pImpl->get()->First();
}

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::Last() {
    return pImpl->get()->Last();
}

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::Next() {
    return pImpl->get()->Next();
}

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::Prev() {
    return pImpl->get()->Prev();
}

std::tuple<bolt::const_bytes, bolt::const_bytes>
Cursor::Seek(bolt::const_bytes seek) {
    return pImpl->get()->Seek(seek);
}

bolt::ErrorCode Cursor::Delete() { return pImpl->get()->Delete(); }

} // namespace bolt
