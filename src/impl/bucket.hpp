#ifndef __BUCKET_HPP__
#define __BUCKET_HPP__

#include "impl/utils.hpp"
#include <memory>
#include <unordered_map>

namespace bolt::impl {

struct page;
struct node;

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
struct bucket {
    impl::pgid root;            // page id of the bucket's root-level page
    std::uint64_t sequence; // monotonically incrementing, used by NextSequence()
};

struct BucketStats {
    int BranchPageN;
    int BranchOverflowN;
    int LeafPageN;
    int LeafOverflowN;
    int KeyN;
    int Depth;
    int BranchAlloc;
    int BranchInuse;
    int LeafAlloc;
    int LeafInuse;
    int BucketN;
    int InlineBucketN;
    int InlineBucketInuse;
};

struct Bucket : public bucket,
                public std::enable_shared_from_this<impl::Bucket> {
    std::weak_ptr<impl::Tx> tx;
    std::map<std::string, impl::BucketPtr> buckets;
    impl::page *page;
    impl::node_ptr rootNode;
    std::map<impl::pgid, impl::node_ptr> nodes;
    float FillPercent;

    explicit Bucket(impl::TxPtr tx);
    impl::TxPtr Tx() const;
    impl::pgid Root() const;
    bool Writable() const;
    impl::CursorPtr Cursor();
    impl::BucketPtr RetrieveBucket(bolt::const_bytes name);
    impl::BucketPtr openBucket(bolt::bytes value);
    std::tuple<impl::BucketPtr, bolt::ErrorCode> CreateBucket(bolt::const_bytes key);
    std::tuple<impl::BucketPtr, bolt::ErrorCode>
    CreateBucketIfNotExists(bolt::const_bytes key);
    bolt::ErrorCode DeleteBucket(bolt::const_bytes key);
    bolt::bytes Get(bolt::const_bytes key);
    bolt::ErrorCode Put(bolt::const_bytes key, bolt::const_bytes value);
    bolt::ErrorCode Delete(bolt::const_bytes key);
    std::uint64_t Sequence();
    bolt::ErrorCode SetSequence(std::uint64_t v);
    std::tuple<std::uint64_t, bolt::ErrorCode> NextSequence();
    bolt::ErrorCode
    ForEach(std::function<bolt::ErrorCode(bolt::bytes key, bolt::bytes val)> &&fn);
    impl::BucketStats Stats();
    void forEachPage(std::function<void(impl::page *, int)> &&fn);
    void forEachPageNode(std::function<void(impl::page *, impl::node_ptr , int)> &&fn);
    void
    _forEachPageNode(impl::pgid pgid, int depth,
                     std::function<void(impl::page *, impl::node_ptr, int)> &fn);
    impl::node_ptr node(impl::pgid pgid, impl::node_ptr parent);
    void rebalance();
    bool inlineable() const;
    int maxInlineBucketSize();
    bolt::ErrorCode spill(std::vector<impl::node_ptr> &hold);
    void free();
    void dereference();
    int maxInlineBucketSize() const;
    std::tuple<impl::page *, impl::node_ptr> pageNode(impl::pgid id);
    std::vector<std::byte> write();
    void dump();
};

constexpr int bucketHeaderSize = sizeof(bucket);
constexpr float minFillPercent = 0.1f;
constexpr float maxFillPercent = 1.0f;

}

#endif  // !__BUCKET_HPP__
