#include "bolt/bolt.hpp"
#include "impl/bucket.hpp"
#include "impl/db.hpp"
#include "impl/tx.hpp"
#include "impl/cursor.hpp"

namespace bolt {

// DB
DB::DB() : pimpl(std::make_shared<impl::DB>()) {}

bolt::ErrorCode DB::Open(std::string path, bool readOnly) {
    return pimpl<impl::DBPtr>::impl()->Open(path, readOnly);
}

bolt::ErrorCode DB::Close() { return pimpl<impl::DBPtr>::impl()->Close(); }

bolt::ErrorCode DB::Update(std::function<bolt::ErrorCode(bolt::Tx)> &&fn) {
    return pimpl<impl::DBPtr>::impl()->Update(
        [fn = std::move(fn)](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            return fn(tx);
        });
}

bolt::ErrorCode DB::Batch(std::function<bolt::ErrorCode(bolt::Tx)> &&fn) {
    return pimpl<impl::DBPtr>::impl()->Batch(
        [fn = std::move(fn)](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          return fn(tx);
        });
}

bolt::ErrorCode DB::View(std::function<bolt::ErrorCode(bolt::Tx)> &&fn) {
    return pimpl<impl::DBPtr>::impl()->View(
        [fn = std::move(fn)](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            return fn(tx);
        });
}

std::tuple<Tx, bolt::ErrorCode> DB::Begin(bool writable) {
    auto [tx, err] = pimpl<impl::DBPtr>::impl()->Begin(writable);
    return std::make_pair(tx, err);
}
bolt::Info DB::Info() { return pimpl<impl::DBPtr>::impl()->Info(); }

bolt::Stats DB::Stats() { return pimpl<impl::DBPtr>::impl()->Stats(); }

bool DB::IsReadOnly() { return pimpl<impl::DBPtr>::impl()->IsReadOnly(); }

std::string DB::Path() { return pimpl<impl::DBPtr>::impl()->Path(); }

// Tx
std::tuple<bolt::Bucket, bolt::ErrorCode>
Tx::CreateBucket(bolt::const_bytes name) {
    auto [b, err] = pimpl<impl::TxPtr>::impl()->CreateBucket(name);
    return std::make_pair(b, err);
}

std::tuple<bolt::Bucket, bolt::ErrorCode>
Tx::CreateBucketIfNotExists(bolt::const_bytes name) {
    auto [b, err] = pimpl<impl::TxPtr>::impl()->CreateBucketIfNotExists(name);
    return std::make_pair(b, err);
}

bolt::ErrorCode Tx::DeleteBucket(bolt::const_bytes name) {
    return pimpl<impl::TxPtr>::impl()->DeleteBucket(name);
}

bolt::ErrorCode Tx::ForEach(
    std::function<bolt::ErrorCode(bolt::const_bytes name, bolt::Bucket b)>
        &&fn) {
    return pimpl<impl::TxPtr>::impl()->ForEach(fn);
}

bolt::Bucket Tx::Bucket(bolt::const_bytes name) {
    return pimpl<impl::TxPtr>::impl()->Bucket(name);
}

bolt::Cursor Tx::Cursor() { return pimpl<impl::TxPtr>::impl()->Cursor(); }

bolt::ErrorCode Tx::Commit() { return pimpl<impl::TxPtr>::impl()->Commit(); }

bolt::ErrorCode Tx::Rollback() {
    return pimpl<impl::TxPtr>::impl()->Rollback();
}

std::future<std::vector<std::string>> Tx::Check() {
    return pimpl<impl::TxPtr>::impl()->Check();
}

bool Tx::Writable() { return pimpl<impl::TxPtr>::impl()->Writable(); }

void Tx::OnCommit(std::function<void()> &&fn) {
    return pimpl<impl::TxPtr>::impl()->OnCommit(std::move(fn));
}

std::uint64_t Tx::Size() { return pimpl<impl::TxPtr>::impl()->Size(); }

bolt::DB Tx::DB() { return pimpl<impl::TxPtr>::impl()->DB(); }

bolt::TxStats Tx::Stats() { return pimpl<impl::TxPtr>::impl()->Stats(); }

// Bucket
std::tuple<bolt::Bucket, bolt::ErrorCode>
Bucket::CreateBucket(bolt::const_bytes key) {
    auto [b, err] = pimpl<impl::BucketPtr>::impl()->CreateBucket(key);
    return std::make_tuple(b, err);
}

std::tuple<bolt::Bucket, bolt::ErrorCode>
Bucket::CreateBucketIfNotExists(bolt::const_bytes key) {
    auto [b, err] = pimpl<impl::BucketPtr>::impl()->CreateBucketIfNotExists(key);
    return std::make_tuple(b, err);
}

bolt::ErrorCode Bucket::DeleteBucket(bolt::const_bytes key) {
    return pimpl<impl::BucketPtr>::impl()->DeleteBucket(key);
}

bolt::const_bytes Bucket::Get(bolt::const_bytes key) {
    return pimpl<impl::BucketPtr>::impl()->Get(key);
}

bolt::ErrorCode Bucket::Put(bolt::const_bytes key, bolt::const_bytes value) {
    return pimpl<impl::BucketPtr>::impl()->Put(key, value);
}

bolt::ErrorCode Bucket::Delete(bolt::const_bytes key) {
    return pimpl<impl::BucketPtr>::impl()->Delete(key);
}

std::uint64_t Bucket::Sequence() {
    return pimpl<impl::BucketPtr>::impl()->Sequence();
}

bolt::ErrorCode Bucket::SetSequence(std::uint64_t v) {
    return pimpl<impl::BucketPtr>::impl()->SetSequence(v);
}

std::tuple<std::uint64_t, bolt::ErrorCode> Bucket::NextSequence() {
    return pimpl<impl::BucketPtr>::impl()->NextSequence();
}

bolt::ErrorCode Bucket::ForEach(
    std::function<bolt::ErrorCode(bolt::const_bytes key, bolt::const_bytes val)>
        &&fn) {
    return pimpl<impl::BucketPtr>::impl()->ForEach(std::move(fn));
}

bolt::Cursor Bucket::Cursor() { return pimpl<impl::BucketPtr>::impl()->Cursor(); }

bolt::Tx Bucket::Tx() { return pimpl<impl::BucketPtr>::impl()->Tx(); }

bolt::Bucket Bucket::RetrieveBucket(bolt::const_bytes name) {
    return pimpl<impl::BucketPtr>::impl()->RetrieveBucket(name);
}

bool Bucket::Writable() { return pimpl<impl::BucketPtr>::impl()->Writable(); }

bolt::BucketStats Bucket::Stats() { return pimpl<impl::BucketPtr>::impl()->Stats(); }

// Cursor
bolt::Bucket Cursor::Bucket() { return pimpl<impl::CursorPtr>::impl()->Bucket(); }

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::First() {
    return pimpl<impl::CursorPtr>::impl()->First();
}

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::Last() {
    return pimpl<impl::CursorPtr>::impl()->Last();
}

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::Next() {
    return pimpl<impl::CursorPtr>::impl()->Next();
}

std::tuple<bolt::const_bytes, bolt::const_bytes> Cursor::Prev() {
    return pimpl<impl::CursorPtr>::impl()->Prev();
}

std::tuple<bolt::const_bytes, bolt::const_bytes>
Cursor::Seek(bolt::const_bytes seek) {
    return pimpl<impl::CursorPtr>::impl()->Seek(seek);
}

bolt::ErrorCode Cursor::Delete() {
    return pimpl<impl::CursorPtr>::impl()->Delete();
}

} // namespace bolt
