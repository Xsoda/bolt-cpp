#ifndef __BUCKET_HPP__
#define __BUCKET_HPP__

#include "common.hpp"
// #include "tx.hpp"

namespace bolt {

struct page;
struct node;

struct bucket {
    bolt::pgid root;
    std::uint64_t sequence;
};

struct Bucket {
    bolt::bucket *bucket;
    // bolt::Tx *tx;
    std::map<std::string, bolt::Bucket*> buckets;
    bolt::page *page;
    bolt::node *rootNode;
    std::map<bolt::pgid, bolt::node*> nodes;
    float FillPercent;

    bolt::node *node(bolt::pgid pgid, const bolt::node *parent) const;
};

const int bucketHeaderSize = sizeof(bucket);
const float minFillPercent = 0.1;
const float maxFillPercent = 1.0;

}

#endif  // !__BUCKET_HPP__
