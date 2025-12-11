#ifndef __BUCKET_HPP__
#define __BUCKET_HPP__

#include "common.hpp"
#include "error.hpp"
#include <memory>


namespace bolt {

struct page;
struct node;

struct bucket {
    bolt::pgid root;
    std::uint64_t sequence;
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

struct Bucket : public std::enable_shared_from_this<bolt::Bucket> {
    bolt::bucket *bucket;
    std::weak_ptr<bolt::Tx> tx;
    std::map<std::string, bolt::BucketPtr> buckets;
    bolt::page *page;
    bolt::node_ptr rootNode;
    std::map<bolt::pgid, bolt::node_ptr> nodes;
    float FillPercent;

    Bucket(bolt::TxPtr tx);
    bolt::TxPtr Tx() const;
    bolt::pgid Root() const;
    bool Writable() const;
    bolt::CursorPtr Cursor();
    bolt::BucketPtr RetrieveBucket(bolt::bytes name);
    bolt::BucketPtr openBucket(bolt::bytes value);
    std::tuple<bolt::BucketPtr, bolt::ErrorCode> CreateBucket(bolt::bytes key);
    std::tuple<bolt::BucketPtr, bolt::ErrorCode>
    CreateBucketIfNotExists(bolt::bytes key);
    bolt::ErrorCode DeleteBucket(bolt::bytes key);
    bolt::bytes Get(bolt::bytes key);
    bolt::ErrorCode Put(bolt::bytes key, bolt::bytes value);
    bolt::ErrorCode Delete(bolt::bytes key);
    std::uint64_t Sequence();
    bolt::ErrorCode SetSequence(std::uint64_t v);
    std::tuple<std::uint64_t, bolt::ErrorCode> NextSequence();
    bolt::ErrorCode
    ForEach(std::function<bolt::ErrorCode(bolt::bytes k, bolt::bytes v)> &&fn);
    bolt::BucketStats Stats();
    void forEachPage(std::function<void(bolt::page *, int)> fn);
    void forEachPageNode(std::function<void(bolt::page *, bolt::node *, int)> &&fn);
    void
    _forEachPageNode(bolt::pgid pgid, int depth,
                     std::function<void(bolt::page *, bolt::node_ptr, int)> &&fn);
    bolt::node_ptr node(bolt::pgid pgid, bolt::node_ptr parent);
    void rebalance();
    bool inlineable() const;
    int maxInlineBucketSize();
    bolt::ErrorCode spill();
    void free();
    void dereference();
    std::tuple<bolt::page *, bolt::node_ptr> pageNode(bolt::pgid id);
    std::vector<std::byte> write();
};

constexpr int bucketHeaderSize = sizeof(bucket);
constexpr float minFillPercent = 0.1;
constexpr float maxFillPercent = 1.0;

}

#endif  // !__BUCKET_HPP__
