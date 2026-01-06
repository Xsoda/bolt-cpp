#include "bucket.hpp"
#include "page.hpp"
#include "tx.hpp"
#include "db.hpp"
#include "node.hpp"
#include "cursor.hpp"
#include "meta.hpp"
#include <cassert>

namespace bolt::impl {

Bucket::Bucket(impl::TxPtr tx): tx(tx) {
    FillPercent = bolt::DefaultFillPercent;
}

impl::TxPtr Bucket::Tx() const {
    return tx.lock();
}

impl::pgid Bucket::Root() const {
    return bucket.root;
}

bool Bucket::Writable() const {
    if (auto t = tx.lock()) {
        return t->writable;
    }
    assert("Tx already invalid in Bucket" && true);
    return false;
}

impl::CursorPtr Bucket::Cursor() {
    return std::make_shared<impl::Cursor>(shared_from_this());
}

impl::node_ptr Bucket::node(impl::pgid pgid, impl::node_ptr parent) {
    auto it = nodes.find(pgid);
    if (it != nodes.end()) {
        return it->second;
    }
    auto n = std::make_shared<impl::node>(shared_from_this(), false, parent);
    if (parent == nullptr) {
        rootNode = n;
    } else {
        parent->children.push_back(n);
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
    nodes[pgid] = n;
    if (t) {
        t->stats.NodeCount++;
    }
    return n;
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
bolt::ErrorCode Bucket::ForEach(
    std::function<bolt::ErrorCode(bolt::bytes key, bolt::bytes val)> &&fn) {
    auto txptr = tx.lock();
    if (!txptr) {
        return bolt::ErrorCode::ErrorTxClosed;
    }
    auto c = Cursor();
    auto [k, v] = c->First();
    while (!k.empty()) {
        auto err = fn(k, v);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        std::tie(k, v) = c->Next();
    }
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
    auto size = impl::pageHeaderSize;
    for (auto &inode : n->inodes) {
        size +=
            impl::leafPageElementSize + inode.key.size() + inode.value.size();
        if ((inode.flags & impl::bucketLeafFlag) != 0) {
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

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
std::tuple<impl::page *, impl::node_ptr> Bucket::pageNode(impl::pgid id) {
    // Inline buckets have a fake page embedded in their value so treat them
    // differently. We'll return the rootNode (if available) or the fake page.
    if (bucket.root == 0) {
        assert("inline bucket non-zero page access" && id != 0);
        if (rootNode) {
            return std::make_tuple(nullptr, rootNode);
        }
        return std::make_tuple(page, nullptr);
    }

    // Check the node cache for non-inline buckets.
    if (!nodes.empty()) {
        auto it = nodes.find(id);
        if (it != nodes.end()) {
            return std::make_tuple(nullptr, it->second);
        }
    }
    // Finally lookup the page from the transaction if no node is materialized.
    auto txptr = tx.lock();
    return std::make_tuple(txptr->page(id), nullptr);
}
}
