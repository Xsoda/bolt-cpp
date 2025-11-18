#ifndef __BUCKET_HPP__
#define __BUCKET_HPP__

#include "common.hpp"

namespace bolt {

struct page;
struct node;
struct Tx;

struct bucket {
    bolt::pgid root;
    std::uint64_t sequence;
};

struct Bucket {
    bolt::bucket *bucket;
    bolt::Tx *tx;
    std::map<std::string, bolt::Bucket*> buckets;
    bolt::page *page;
    bolt::node *rootNode;
    std::map<bolt::pgid, bolt::node*> nodes;
    float FillPercent;

    Bucket(bolt::Tx *tx);
    bolt::Tx *Tx() const;
    bolt::pgid Root() const;
    bool Writable() const;
    bolt::node *node(bolt::pgid pgid, bolt::node *parent);
};

constexpr int bucketHeaderSize = sizeof(bucket);
constexpr float minFillPercent = 0.1;
constexpr float maxFillPercent = 1.0;

}

#endif  // !__BUCKET_HPP__
