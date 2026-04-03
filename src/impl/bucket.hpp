#ifndef __BUCKET_HPP__
#define __BUCKET_HPP__

#include "impl/utils.hpp"
#include <map>
#include <memory>

namespace bolt::impl {

struct page;
struct node;

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
struct bucket {
    impl::pgid root;        // page id of the bucket's root-level page
    std::uint64_t sequence; // monotonically incrementing, used by NextSequence()
};

struct Bucket : public bucket, public std::enable_shared_from_this<impl::Bucket> {
    std::weak_ptr<impl::Tx> tx;                     // the associated translate
    std::map<std::string, impl::BucketPtr> buckets; // subbucket cache
    impl::page *page;                               // inline page reference
    impl::node_ptr rootNode;                        // materialized node for the root page.
    std::map<impl::pgid, impl::node_ptr> nodes;     // node cache
    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    float FillPercent;

    explicit Bucket(impl::TxPtr tx);

    // Tx returns the tx of the bucket.
    impl::TxPtr Tx() const;
    impl::pgid Root() const;
    bool Writable() const;
    // Cursor creates a cursor associated with the bucket.
    // The cursor is only valid as long as the transaction is open.
    // Do not use a cursor after the transaction is closed.
    impl::CursorPtr Cursor();
    // Bucket retrieves a nested bucket by name.
    // Returns nil if the bucket does not exist.
    // The bucket instance is only valid for the lifetime of the transaction.
    impl::BucketPtr RetrieveBucket(bolt::const_bytes name);
    // Helper method that re-interprets a sub-bucket value
    // from a parent into a Bucket
    impl::BucketPtr openBucket(bolt::bytes value);
    // CreateBucket creates a new bucket at the given key and returns the new bucket.
    // Returns an error if the key already exists, if the bucket name is blank, or if the bucket
    // name is too long. The bucket instance is only valid for the lifetime of the transaction.
    std::tuple<impl::BucketPtr, bolt::ErrorCode> CreateBucket(bolt::const_bytes key);
    // CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a
    // reference to it. Returns an error if the bucket name is blank, or if the bucket name is too
    // long. The bucket instance is only valid for the lifetime of the transaction.
    std::tuple<impl::BucketPtr, bolt::ErrorCode> CreateBucketIfNotExists(bolt::const_bytes key);
    // DeleteBucket deletes a bucket at the given key.
    // Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
    bolt::ErrorCode DeleteBucket(bolt::const_bytes key);
    // Get retrieves the value for a key in the bucket.
    // Returns a nil value if the key does not exist or if the key is a nested bucket.
    // The returned value is only valid for the life of the transaction.
    bolt::const_bytes Get(bolt::const_bytes key);
    // Put sets the value for a key in the bucket.
    // If the key exist then its previous value will be overwritten.
    // Supplied value must remain valid for the life of the transaction.
    // Returns an error if the bucket was created from a read-only transaction, if the key is blank,
    // if the key is too large, or if the value is too large.
    bolt::ErrorCode Put(bolt::const_bytes key, bolt::const_bytes value);
    // Delete removes a key from the bucket.
    // If the key does not exist then nothing is done and a nil error is returned.
    // Returns an error if the bucket was created from a read-only transaction.
    bolt::ErrorCode Delete(bolt::const_bytes key);
    // Sequence returns the current integer for the bucket without incrementing it.
    std::uint64_t Sequence();
    // SetSequence updates the sequence number for the bucket.
    bolt::ErrorCode SetSequence(std::uint64_t v);
    // NextSequence returns an autoincrementing integer for the bucket.
    std::tuple<std::uint64_t, bolt::ErrorCode> NextSequence();
    // ForEach executes a function for each key/value pair in a bucket.
    // If the provided function returns an error then the iteration is stopped and
    // the error is returned to the caller. The provided function must not modify
    // the bucket; this will result in undefined behavior.
    bolt::ErrorCode
    ForEach(std::function<bolt::ErrorCode(bolt::const_bytes key, bolt::const_bytes val)> &&fn);
    // Stat returns stats on a bucket.
    bolt::BucketStats Stats();
    // forEachPage iterates over every page in a bucket, including inline pages.
    void forEachPage(std::function<void(impl::page *, int)> &&fn);
    // forEachPageNode iterates over every page (or node) in a bucket.
    // This also includes inline pages.
    void forEachPageNode(std::function<void(impl::page *, impl::node_ptr, int)> &&fn);
    void _forEachPageNode(impl::pgid pgid, int depth,
                          std::function<void(impl::page *, impl::node_ptr, int)> &fn);
    // node creates a node from a page and associates it with a given parent.
    impl::node_ptr node(impl::pgid pgid, impl::node_ptr parent);
    // rebalance attempts to balance all nodes.
    void rebalance();
    // inlineable returns true if a bucket is small enough to be written inline
    // and if it contains no subbuckets. Otherwise returns false.
    bool inlineable() const;
    // Returns the maximum total size of a bucket to make it a candidate for inlining.
    int maxInlineBucketSize();
    // spill writes all the nodes for this bucket to dirty pages.
    bolt::ErrorCode spill(std::vector<impl::node_ptr> &hold);
    // free recursively frees all pages in the bucket.
    void free();
    // dereference removes all references to the old mmap.
    void dereference();
    // Returns the maximum total size of a bucket to make it a candidate for inlining.
    int maxInlineBucketSize() const;
    // pageNode returns the in-memory node, if it exists.
    // Otherwise returns the underlying page.
    std::tuple<impl::page *, impl::node_ptr> pageNode(impl::pgid id);
    // write allocates and writes a bucket to a byte slice.
    std::vector<std::byte> write();
    void dump();
};

constexpr int bucketHeaderSize = sizeof(bucket);
constexpr float minFillPercent = 0.1f;
constexpr float maxFillPercent = 1.0f;

} // namespace bolt::impl

#endif // !__BUCKET_HPP__
