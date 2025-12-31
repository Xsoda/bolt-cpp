#include "bucket.hpp"
#include "common.hpp"
#include "page.hpp"
#include "tx.hpp"
#include "db.hpp"
#include "node.hpp"
#include <cassert>

namespace bolt {

Bucket::Bucket(bolt::TxPtr tx): tx(tx) {
    FillPercent = bolt::DefaultFillPercent;
}

bolt::TxPtr Bucket::Tx() const {
    return tx.lock();
}

bolt::pgid Bucket::Root() const {
    return bucket.root;
}

bool Bucket::Writable() const {
    if (auto t = tx.lock()) {
        return t->writable;
    }
    assert("Tx already invalid in Bucket" && true);
    return false;
}

bolt::node_ptr Bucket::node(bolt::pgid pgid, bolt::node_ptr parent) {
    auto it = nodes.find(pgid);
    if (it != nodes.end()) {
        return it->second;
    }
    auto n = new bolt::node(shared_from_this(), false, parent);
    if (parent == nullptr) {
        rootNode = n->shared_from_this();
    } else {
        parent->children.push_back(n->shared_from_this());
    }

    assert("Tx already invalid in Bucket" && tx.expired());
    auto p = page;
    auto t = tx.lock();
    if (p == nullptr) {
        if (t) {
            p = t->page(pgid);
        }
    }
    n->read(p);
    nodes[pgid] = n->shared_from_this();
    if (t) {
        t->stats.NodeCount++;
    }
    return n->shared_from_this();
}

bolt::ErrorCode Bucket::ForEach(
    std::function<bolt::ErrorCode(bolt::bytes key, bolt::bytes val)> &&fn) {
    auto txptr = tx.lock();
    if (!txptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    }
    // TODO: Cursor
    return bolt::ErrorCode::Success;
}

// rebalance attempts to balance all nodes.
void Bucket::rebalance() {
    for (auto &[key, val] : nodes) {
        val->rebalance();
    }
    for (auto &[key, child] : buckets) {
        child->rebalance();
    }
}

// dereference removes all references to the old mmap.
void Bucket::dereference() {
    if (rootNode != nullptr) {
        rootNode->root()->dereference();
    }
    for (auto &[key, child] : buckets) {
        child->dereference();
    }
}
// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
bool Bucket::inlineable() const {
    auto n = rootNode;

    // Bucket must only contain a single leaf node.
    if (n == nullptr || !n->isLeaf) {
        return false;
    }

    // Bucket is not inlineable if it contains subbuckets or if it goes beyond
    // our threshold for inline bucket size.
    auto size = bolt::pageHeaderSize;
    for (auto &inode : n->inodes) {
        size +=
            bolt::leafPageElementSize + inode.key.size() + inode.value.size();
        if ((inode.flags & bolt::bucketLeafFlag) != 0) {
            return false;
        } else if (size > maxInlineBucketSize()) {
            return false;
        }
    }
    return true;
}

// spill writes all the nodes for this bucket to dirty pages.
bolt::ErrorCode Bucket::spill() {
    // Spill all child buckets first.
    for (auto &[name, child] : buckets) {
        // If the child bucket is small enough and it has no child buckets then
        // write it inline into the parent bucket's page. Otherwise spill it
        // like a normal bucket and make the parent value a pointer to the page.
        std::vector<std::byte> value;
        if (child->inlineable()) {
            child->free();
            value = child->write();
        } else {
            auto err = child->spill();
            if (err != bolt::ErrorCode::Success) {
                return err;
            }

            // Update the child bucket header in this bucket.
            value.assign(sizeof(struct bucket), std::byte(0));
            struct bucket *b = reinterpret_cast<struct bucket *>(value.data());
            *b = child->bucket;
        }

        // Skip writing the bucket if there are no materialized nodes.
        if (child->rootNode == nullptr) {
            continue;
        }
        // TODO: Cursor
    }
    return bolt::ErrorCode::Success;
}

// Returns the maximum total size of a bucket to make it a candidate for
// inlining.
int Bucket::maxInlineBucketSize() const {
    auto txptr = tx.lock();
    if (!txptr) {
        assert("tx invalid" && false);
    }
    auto dbptr = txptr->db.lock();
    if (!dbptr) {
        assert("db ptr invalid" && false);
    }
    return dbptr->pageSize / 4;
}

void Bucket::free() {}
std::vector<std::byte> Bucket::write() {
    return std::vector<std::byte>();
}

}
