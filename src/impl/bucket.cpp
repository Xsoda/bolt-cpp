#include "impl/bucket.hpp"
#include "impl/page.hpp"
#include "impl/db.hpp"
#include "impl/node.hpp"
#include "impl/cursor.hpp"
#include "impl/meta.hpp"
#include <algorithm>

namespace bolt::impl {

Bucket::Bucket(impl::TxPtr tx) : tx(tx) {
    page = nullptr;
    root = 0;
    sequence = 0;
    FillPercent = bolt::DefaultFillPercent;
}

impl::TxPtr Bucket::Tx() const {
    return tx.lock();
}

impl::pgid Bucket::Root() const {
    return root;
}

bool Bucket::Writable() const {
    if (auto t = tx.lock()) {
        return t->writable;
    }
    _assert(false, "Tx invalid in Bucket");
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

    _assert(!tx.expired(), "Tx already invalid in Bucket");
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
    auto nit = nodes.begin();
    while (nit != nodes.end()) {
        auto npgid = nit->first;
        nit->second->rebalance();
        nit = nodes.upper_bound(npgid);
    }
    auto bit = buckets.begin();
    while (bit != buckets.end()) {
        auto bname = bit->first;
        bit->second->rebalance();
        bit = buckets.upper_bound(bname);
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
bolt::ErrorCode Bucket::spill(std::vector<impl::node_ptr> &hold) {
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
            auto err = child->spill(hold);
            if (err != bolt::ErrorCode::Success) {
                return err;
            }

            // Update the child bucket header in this bucket.
            value.assign(sizeof(struct bucket), std::byte(0));
            struct bucket *b = reinterpret_cast<struct bucket *>(value.data());
            b->root = child->root;
            b->sequence = child->sequence;
        }

        // Skip writing the bucket if there are no materialized nodes.
        if (child->rootNode == nullptr) {
            continue;
        }

        // Update parent node.
        std::vector<std::byte> key(name.size());
        std::transform(name.begin(), name.end(), key.begin(),
                       [](char c) -> std::byte {
                           return std::byte(c);
                       });
        auto c = Cursor();
        auto [k, v, flags] = c->seek(key);
        if (!std::is_eq(std::lexicographical_compare_three_way(
                key.begin(), key.end(), k.begin(), k.end()))) {
            _assert(false, "misplaced bucket header");
        }
        if ((flags & bolt::impl::bucketLeafFlag) == 0) {
            _assert(false, "unexpceted bucket header falg");
        }
        c->node()->put(key, key, value, 0, bolt::impl::bucketLeafFlag);
    }
    // Ignore if there's not a masterialized root node.
    if (rootNode == nullptr) {
        return bolt::ErrorCode::Success;
    }

    // Spill nodes.
    if (auto err = rootNode->spill(hold); err != bolt::ErrorCode::Success) {
        return err;
    }
    rootNode = rootNode->root();

    // Update the root node for this bucket.
    auto txptr = tx.lock();
    if (rootNode->pgid >= txptr->meta.pgid) {
        _assert(false, "pgid ({}) above high water mark ({})", rootNode->pgid, txptr->meta.pgid);
    }
    root = rootNode->pgid;
    log_debug("* set bucket root {}", root);
    return bolt::ErrorCode::Success;
}

// Returns the maximum total size of a bucket to make it a candidate for
// inlining.
int Bucket::maxInlineBucketSize() const {
    auto txptr = tx.lock();
    if (!txptr) {
        _assert(false, "tx invalid");
    }
    auto dbptr = txptr->db.lock();
    if (!dbptr) {
        _assert(false, "db ptr invalid");
    }
    return dbptr->pageSize / 4;
}

// free recursively frees all pages in the bucket.
void Bucket::free() {
    if (root == 0) {
        return;
    }
    auto txptr = tx.lock();
    if (!txptr) {
        _assert(false, "txptr invalid");
        return;
    }
    auto dbptr = txptr->db.lock();
    if (!dbptr) {
        _assert(false, "dbptr invalid");
        return;
    }
    forEachPageNode(
        [dbptr, txptr](bolt::impl::page *p, bolt::impl::node_ptr n, int _) {
            if (p != nullptr) {
                dbptr->freelist->free(txptr->meta.txid, p);
            } else {
                n->free();
            }
        });
    root = 0;
}

// write allocates and writes a bucket to a byte slice.
std::vector<std::byte> Bucket::write() {
    // Allocate the appropriate size.
    auto n = rootNode;
    std::vector<std::byte> value;
    value.assign(bolt::impl::bucketHeaderSize + n->size(), std::byte(0));

    // Write a bucket header.
    auto b = reinterpret_cast<bolt::impl::bucket *>(value.data());
    b->root = root;
    b->sequence = sequence;

    // Convert byte slice to a fake page and write the root node
    auto p = reinterpret_cast<bolt::impl::page *>(value.data() +
                                                  bolt::impl::bucketHeaderSize);
    n->write(p);
    return value;
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
std::tuple<impl::page *, impl::node_ptr> Bucket::pageNode(impl::pgid id) {
    // Inline buckets have a fake page embedded in their value so treat them
    // differently. We'll return the rootNode (if available) or the fake page.
    if (root == 0) {
        _assert(id == 0, "inline bucket non-zero page access: {} != 0", id);
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

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
impl::BucketPtr Bucket::RetrieveBucket(bolt::bytes name) {
    std::string key{reinterpret_cast<char*>(name.data()), name.size()};
    auto it = buckets.find(key);
    if (it != buckets.end()) {
        return it->second;
    }

    // Move cursor to key.
    auto c = Cursor();
    auto [k, v, flags] = c->seek(name);

    // Return nil if the key doesn't exist or it is not a bucket.
    if (!std::is_eq(std::lexicographical_compare_three_way(
            name.begin(), name.end(), k.begin(), k.end())) ||
        (flags & bolt::impl::bucketLeafFlag) == 0) {
        return nullptr;
    }

    // Otherwise create a bucket and cache it.
    auto child = openBucket(v);
    buckets.insert(std::make_pair(key, child));
    return child;
}

// Helper method that re-interprets a sub-bucket value
// from a parent into a Bucket
impl::BucketPtr Bucket::openBucket(bolt::bytes value) {
    auto txptr = tx.lock();
    if (!txptr) {
        _assert(false, "tx closed");
        return nullptr;
    }
    auto child = std::make_shared<impl::Bucket>(txptr);
    auto b = reinterpret_cast<bolt::impl::bucket *>(value.data());
    child->root = b->root;
    child->sequence = b->sequence;

    // Save a reference to the inline page if the bucket is inline.
    if (child->root == 0) {
        child->page = reinterpret_cast<bolt::impl::page*>(value.data() + bolt::impl::bucketHeaderSize);
    }
    return child;
}

// CreateBucket creates a new bucket at the given key and returns the new
// bucket. Returns an error if the key already exists, if the bucket name is
// blank, or if the bucket name is too long. The bucket instance is only valid
// for the lifetime of the transaction.
std::tuple<impl::BucketPtr, bolt::ErrorCode>
Bucket::CreateBucket(bolt::bytes key) {
    if (tx.expired()) {
        return std::make_tuple(nullptr, bolt::ErrorCode::ErrorTxClosed);
    }
    auto txptr = tx.lock();
    if (txptr->db.expired()) {
        return std::make_tuple(nullptr, bolt::ErrorCode::ErrorTxClosed);
    } else if (!txptr->writable) {
        return std::make_tuple(nullptr, bolt::ErrorCode::ErrorTxNotWritable);
    } else if (key.empty()) {
        return std::make_tuple(nullptr, bolt::ErrorCode::ErrorBucketNameRequired);
    }

    // Move cursor to correct position.
    auto c = Cursor();
    auto [k, v, flags] = c->seek(key);

    // Return an error if there is an existing key.
    if (std::is_eq(std::lexicographical_compare_three_way(
            key.begin(), key.end(), k.begin(), k.end()))) {
        if ((flags & bolt::impl::bucketLeafFlag) != 0) {
            return std::make_tuple(nullptr, bolt::ErrorCode::ErrorBucketExists);
        }
        return std::make_tuple(nullptr, bolt::ErrorCode::ErrorIncompatiableValue);
    }

    // Create empty, inline bucket.
    auto b = std::make_shared<impl::Bucket>(nullptr);
    b->rootNode = std::make_shared<impl::node>(true);
    b->FillPercent = bolt::DefaultFillPercent;
    auto value = b->write();

    c->node()->put(key, key, value, 0, bolt::impl::bucketLeafFlag);

    // Since subbuckets are not allowed on inline buckets, we need to
    // dereference the inline page, if it exists. This will cause the bucket
    // to be treated as a regular, non-inline bucket for the rest of the tx.
    this->page = nullptr;
    return std::make_tuple(RetrieveBucket(key), bolt::ErrorCode::Success);
}

// forEachPageNode iterates over every page (or node) in a bucket.
// This also includes inline pages.
void Bucket::forEachPageNode(
    std::function<void(impl::page *, impl::node_ptr, int)> &&fn) {
    // If we have an inline page or root node then just use that.
    if (page != nullptr) {
        fn(page, nullptr, 0);
        return;
    }
    return _forEachPageNode(root, 0, fn);
}

void Bucket::_forEachPageNode(
    impl::pgid pgid, int depth,
    std::function<void(impl::page *, impl::node_ptr, int)> &fn) {
    auto [p, n] = pageNode(pgid);

    // Execute function.
    fn(p, n, depth);

    // Recursively loop over children.
    if (p != nullptr) {
        if ((p->flags & bolt::impl::branchPageFlag) != 0) {
            for (size_t i = 0; i < p->count; i++) {
                auto elem = p->branchPageElement(std::uint16_t(i));
                _forEachPageNode(elem->pgid, depth+1, fn);
            }
        }
    } else {
        if (!n->isLeaf) {
            for (auto &inode : n->inodes) {
                _forEachPageNode(inode.pgid, depth+1, fn);
            }
        }
    }
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and
// returns a reference to it. Returns an error if the bucket name is blank, or
// if the bucket name is too long. The bucket instance is only valid for the
// lifetime of the transaction.
std::tuple<impl::BucketPtr, bolt::ErrorCode>
Bucket::CreateBucketIfNotExists(bolt::bytes key) {
    auto [child, err] = CreateBucket(key);
    if (err == bolt::ErrorCode::ErrorBucketExists) {
        return std::make_tuple(RetrieveBucket(key), bolt::ErrorCode::Success);
    } else if (err != bolt::ErrorCode::Success) {
        return std::make_tuple(nullptr, err);
    }
    return std::make_tuple(child, err);
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exists, or if the key represents a
// non-bucket value.
bolt::ErrorCode Bucket::DeleteBucket(bolt::bytes key) {
    auto txptr = tx.lock();
    if (!txptr || txptr->db.expired()) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!Writable()) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    }

    // Move cursor to correct position.
    auto c = Cursor();
    auto [k, v, flags] = c->seek(key);

    // Return an error if bucket doesn't exist or is not a bucket.
    if (!std::is_eq(std::lexicographical_compare_three_way(
            key.begin(), key.end(), k.begin(), k.end()))) {
        return bolt::ErrorCode::ErrorBucketNotFound;
    } else if ((flags & bolt::impl::bucketLeafFlag) == 0) {
        return bolt::ErrorCode::ErrorIncompatiableValue;
    }

    // Recursively delete all child buckets.
    auto child = RetrieveBucket(key);
    auto err =
        child->ForEach([&](bolt::bytes ck, bolt::bytes cv) -> bolt::ErrorCode {
          if (cv.empty()) {
            auto err = child->DeleteBucket(ck);
            if (err != bolt::ErrorCode::Success) {
              return err;
            }
          }
          return bolt::ErrorCode::Success;
        });
    if (err != bolt::ErrorCode::Success) {
        return err;
    }

    // Remove cached copy.
    auto it = buckets.find(
        std::string(reinterpret_cast<char *>(key.data()), key.size()));
    if (it != buckets.end()) {
        buckets.erase(it);
    }

    // Release all bucket pages to freelist.
    child->nodes.clear();
    child->rootNode = nullptr;
    child->free();

    // Delete the node if we have a matching key.
    c->node()->del(key);

    return bolt::ErrorCode::Success;
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested
// bucket. The returned value is only valid for the life of the transaction.
bolt::bytes Bucket::Get(bolt::bytes key) {
    auto c = Cursor();
    auto [k, v, flags] = c->seek(key);

    // Return nil if this is a bucket.
    if ((flags & bolt::impl::bucketLeafFlag) != 0) {
        return bolt::bytes{};
    }

    // If our target node isn't the same key as what's passed in then return
    // nil.
    if (!std::is_eq(std::lexicographical_compare_three_way(
            key.begin(), key.end(), k.begin(), k.end()))) {
        return bolt::bytes{};
    }
    return v;
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction, if
// the key is blank, if the key is too large, or if the value is too large.
bolt::ErrorCode Bucket::Put(bolt::bytes key, bolt::bytes value) {
    if (tx.expired()) {
        return bolt::ErrorCode::ErrorTxClosed;
    }
    auto txptr = tx.lock();
    if (txptr->db.expired()) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!Writable()) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    } else if (key.size() == 0) {
        return bolt::ErrorCode::ErrorKeyRequired;
    } else if (key.size() > bolt::MaxKeySize) {
        return bolt::ErrorCode::ErrorKeyTooLarge;
    } else if (value.size() > bolt::MaxValueSize) {
        return bolt::ErrorCode::ErrorValueTooLarge;
    }

    // Move cursor to correct position.
    auto c = Cursor();
    auto [k, v, flags] = c->seek(key);

    // Return an error if there is an existing key with a bucket value.
    if (std::is_eq(std::lexicographical_compare_three_way(
            key.begin(), key.end(), k.begin(), k.end())) &&
        (flags & bolt::impl::bucketLeafFlag) != 0) {
        return bolt::ErrorCode::ErrorIncompatiableValue;
    }

    // Insert into node.
    c->node()->put(key, key, value, 0, 0);
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode Bucket::Delete(bolt::bytes key) {
    if (tx.expired()) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!Writable()) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    }

    // Move cursor to correct position.
    auto c = Cursor();
    auto [k, v, flags] = c->seek(key);

    // Return an error if there is already existing bucket value.
    if ((flags & bolt::impl::bucketLeafFlag) != 0) {
        return bolt::ErrorCode::ErrorIncompatiableValue;
    }

    // Delete the node if we have a matching key.
    c->node()->del(key);
    return bolt::ErrorCode::Success;
}

// Sequence returns the current integer for the bucket without incrementing it.
std::uint64_t Bucket::Sequence() { return sequence; }

bolt::ErrorCode Bucket::SetSequence(std::uint64_t v) {
    if (tx.expired()) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!Writable()) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    }

    // Materialize the root node if it hasn't been already so that the
    // bucket will be saved during commit.
    if (rootNode == nullptr) {
        std::ignore = node(root, nullptr);
    }

    // Increment and return the sequence.
    sequence = v;
    return bolt::ErrorCode::Success;
}

// NextSequence returns an autoincrementing integer for the bucket.
std::tuple<std::uint64_t, bolt::ErrorCode> Bucket::NextSequence() {
    if (tx.expired()) {
        return std::make_tuple(0,bolt::ErrorCode::ErrorTxClosed);
    } else if (!Writable()) {
        return std::make_tuple(0,bolt::ErrorCode::ErrorTxNotWritable);
    }

    // Materialize the root node if it hasn't been already so that the
    // bucket will be saved during commit.
    if (rootNode == nullptr) {
        std::ignore = node(root, nullptr);
    }

    // Increment and return the sequence.
    sequence++;
    return std::make_tuple(sequence, bolt::ErrorCode::Success);
}

// forEachPage iterates over every page in a bucket, including inline pages.
void Bucket::forEachPage(std::function<void(impl::page *, int)> &&fn) {
    // If we have an inline page then just use that.
    if (page == nullptr) {
        fn(page, 0);
        return;
    }
    // Otherwise traverse the page hierarchy.
    auto txptr = tx.lock();
    txptr->forEachPage(root, 0, std::forward<decltype(fn)>(fn));
}

void Bucket::dump() {
    log_debug("[BUCKET root {}]", root);
    if (rootNode) {
        rootNode->dump();
    }
}
}
