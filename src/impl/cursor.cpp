#include "impl/tx.hpp"
#include "impl/cursor.hpp"
#include "impl/node.hpp"
#include "impl/page.hpp"
#include "impl/bucket.hpp"
#include "impl/utils.hpp"
#include <algorithm>
#include <iterator>

namespace bolt::impl {

bool elemRef::isLeaf() const {
    if (auto n = node.lock()) {
        return n->isLeaf;
    }
    return (this->page->flags & impl::leafPageFlag) != 0;
}

int elemRef::count() const {
    if (auto n = node.lock()) {
        return static_cast<int>(n->inodes.size());
    }
    return this->page->count;
}

impl::node_ptr Cursor::node() const {
    size_t len = stack.size();
    _assert(len > 0, "accessing a node with a zero-length cursor stack");
    auto &ref = stack.back();
    if (auto n = ref.node.lock()) {
        if (ref.isLeaf()) {
            return n->shared_from_this();
        }
    }
    auto &f = stack.front();
    _assert(!bucket.expired(), "bucket already expired");
    auto b = bucket.lock();
    auto n = stack.front().node.lock();
    if (!n) {
        n = b->node(f.page->id, nullptr);
    }
    for (size_t i = 0; i < len - 1; i++) {
        auto &ref = stack.at(i);
        _assert(!n->isLeaf, "expected branch node");
        n = n->childAt(ref.index);
    }
    _assert(n->isLeaf, "expected leaf node");
    return n->shared_from_this();
}

impl::BucketPtr Cursor::Bucket() {
    return bucket.lock();
}

// First moves the cursor to the first item in the bucket and returns its key
// and value. If the bucket is empty then a nil key and value are returned. The
// returned key and value are only valid for the life of the transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::First() {
    _assert(!bucket.expired(), "bucket expired");
    auto bptr = bucket.lock();
    _assert(!bptr->tx.expired(), "tx closed");
    stack.clear();

    auto [p, n] = bptr->pageNode(bptr->root);
    stack.push_back(elemRef(p, n, 0));
    first();

    // If we land on an empty page then move to the next value.
    // https://github.com/boltdb/bolt/issues/450
    if (stack.back().count() == 0) {
        next();
    }

    auto [k, v, flags] = keyValue();
    if ((flags & impl::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

// Last moves the cursor to the last item in the bucket and returns its key and
// value. If the bucket is empty then a nil key and value are returned. The
// returned key and value are only valid for the life of the transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::Last() {
    auto bptr = bucket.lock();
    _assert(!bucket.expired(), "bucket ptr invalid");
    auto txptr = bptr->tx.lock();
    _assert(!bptr->tx.expired(), "tx closed");
    stack.clear();

    auto [p, n] = bptr->pageNode(bptr->root);
    stack.push_back(elemRef(p, n, 0));
    auto &ref = stack.back();
    ref.index = ref.count() - 1;

    last();
    auto [k, v, flags] = keyValue();
    if ((flags & bolt::impl::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

// Next moves the cursor to the next item in the bucket and returns its key and
// value. If the cursor is at the end of the bucket then a nil key and value are
// returned. The returned key and value are only valid for the life of the
// transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::Next() {
    _assert(!bucket.expired(), "bucket already expired");
    auto bptr = bucket.lock();
    _assert(!bptr->tx.expired(), "tx closed");

    auto [k, v, flags] = next();
    if ((flags & impl::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

// Prev moves the cursor to the previous item in the bucket and returns its key
// and value. If the cursor is at the beginning of the bucket then a nil key and
// value are returned. The returned key and value are only valid for the life of
// the transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::Prev() {
    _assert(!bucket.expired(), "bucket already expired");
    auto bptr = bucket.lock();
    _assert(!bptr->tx.expired(), "tx closed");

    // Attempt to move back one element until we're successful.
    // Move up the stack as we hit the beginning of each page in our stack.
    for (auto elem = stack.rbegin();
         elem != stack.rend();) {
        if (elem->index > 0) {
            elem->index--;
            break;
        }
        auto it = stack.erase(elem.base() - 1, stack.end());
        elem = std::reverse_iterator(it);
    }

    // If we've hit the end then return nil.
    if (stack.size() == 0) {
        return std::make_tuple(bolt::bytes(), bolt::bytes());
    }

    // Move down the stack to find the last element of the last leaf under this
    // branch.
    last();
    auto [k, v, flags] = keyValue();
    if ((flags & bolt::impl::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

// Seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, a nil key is returned.
// The returned key and value are only valid for the life of the transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::Seek(bolt::bytes seek) {
    auto [k, v, flags] = this->seek(seek);

    // If we ended up after the last element of a page then move to the next
    // one.
    if (auto &ref = stack.back(); ref.index >= ref.count()) {
        std::tie(k, v, flags) = next();
    }

    if (k.empty()) {
        return std::make_tuple(bolt::bytes(), bolt::bytes());
    } else if ((flags & bolt::impl::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

bolt::ErrorCode Cursor::Delete() {
    auto bktptr = bucket.lock();
    _assert(!bucket.expired(), "bucket invalid");
    auto txptr = bktptr->tx.lock();
    _assert(!bktptr->tx.expired(), "tx invalid");
    if (txptr->db.expired()) {
        return bolt::ErrorCode::ErrorTxClosed;
    } else if (!bktptr->Writable()) {
        return bolt::ErrorCode::ErrorTxNotWritable;
    }

    auto [k, v, flags] = keyValue();
    // Return an error if current value is a bucket.
    if ((flags & bolt::impl::bucketLeafFlag) != 0) {
        return bolt::ErrorCode::ErrorIncompatiableValue;
    }
    node()->del(k);

    return bolt::ErrorCode::Success;
}

std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> Cursor::keyValue() {
    auto &ref = stack.back();
    if (ref.count() == 0 || ref.index >= ref.count()) {
        return std::make_tuple<bolt::bytes, bolt::bytes, std::uint32_t>(bolt::bytes(), bolt::bytes(), 0);
    }
    if (auto n = ref.node.lock()) {
        auto &inode = n->inodes.at(ref.index);
        return std::make_tuple(inode.key, inode.value, inode.flags);
    }

    impl::leafPageElement *elem = ref.page->leafPageElement((uint16_t)ref.index);
    return std::make_tuple(elem->key(), elem->value(), elem->flags);
}

std::tuple<bolt::bytes, bolt::bytes, std::uint32_t>
Cursor::seek(bolt::bytes k) {
    _assert(!bucket.expired(), "Bucket already expired in Cursor");
    auto b = bucket.lock();
    _assert(!b->tx.expired(), "tx closed");
    stack.clear();
    search(k, b->root);
    if (auto &ref = stack.back(); ref.index >= ref.count()) {
        return std::make_tuple(bolt::bytes(), bolt::bytes(), 0);
    }
    return keyValue();
}

// first moves the cursor to the first leaf element under the last page in the
// stack.
void Cursor::first() {
    while (true) {
        // Exit when we hit a leaf page.
        auto &ref = stack.back();
        if (ref.isLeaf()) {
            break;
        }

        // Keep adding pages pointing to the first element to the stack.
        impl::pgid pgid;
        if (!ref.node.expired()) {
            auto n = ref.node.lock();
            pgid = n->inodes[ref.index].pgid;
        } else {
            pgid = ref.page->branchPageElement((std::uint16_t)ref.index)->pgid;
        }
        auto b = bucket.lock();
        if (b) {
            auto [page, node] = b->pageNode(pgid);
            stack.push_back(elemRef(page, node, 0));
        }
    }
}

// last moves the cursor to the last leaf element under the last page in the
// stack.
void Cursor::last() {
    while (true) {
        // Exit when we hit a leaf page.
        auto &ref = stack.back();
        if (ref.isLeaf()) {
            break;
        }

        // Keep adding pages pointing to the last element in the stack.
        impl::pgid pgid;
        if (!ref.node.expired()) {
            auto n = ref.node.lock();
            pgid = n->inodes[ref.index].pgid;
        } else {
            pgid = ref.page->branchPageElement((std::uint16_t)ref.index)->pgid;
        }
        auto b = bucket.lock();
        if (b) {
            auto [page, node] = b->pageNode(pgid);
            elemRef next(page, node, 0);
            next.index = next.count() - 1;
            stack.push_back(next);
        }
    }
}
// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns
// nil.
std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> Cursor::next() {
    while (true) {
        // Attempt to move over one element until we're successful.
        // Move up the stack as we hit the end of each page in our stack.
        auto it = stack.rbegin();
        for (; it != stack.rend(); it++) {
            if (it->index < it->count() - 1) {
                it->index++;
                break;
            }
        }

        // If we've hit the root page then stop and return. This will leave the
        // cursor on the last element of the last page.
        if (it == stack.rend()) {
            return std::make_tuple(bolt::bytes(), bolt::bytes(), 0);
        }

        // Otherwise start from where we left off in the stack and find the
        // first element of the first leaf page.
        auto size = std::distance(stack.rbegin(), it);
        stack.erase(stack.end() - size, stack.end());
        first();

        // If this is an empty page then restart and move back up the stack.
        // https://github.com/boltdb/bolt/issues/450
        auto &ref = stack.back();
        if (ref.count() == 0) {
            continue;
        }
        return keyValue();
    }
}

// search recursively performs a binary search against a given page/node until
// it finds a given key.
void Cursor::search(bolt::bytes key, impl::pgid pgid) {
    auto b = bucket.lock();
    auto [p, n] = b->pageNode(pgid);
    if (p != nullptr && (p->flags & (impl::branchPageFlag | impl::leafPageFlag)) == 0) {
        _assert(false, "invalid page type: {}: {}", p->id, p->flags);
    }
    elemRef e{p, n};
    stack.push_back(e);
    // If we're on a leaf page/node then find the specific node.
    if (e.isLeaf()) {
        nsearch(key);
        return;
    }
    if (n) {
        searchNode(key, n);
        return;
    }
    searchPage(key, p);
}

/*
func (c *Cursor) searchNode(key []byte, n *node) {
        var exact bool
        index := sort.Search(len(n.inodes), func(i int) bool {
                // TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
                // sort.Search() finds the lowest index where f() != -1 but we need the highest index.
                ret := bytes.Compare(n.inodes[i].key, key) if ret == 0 {
                        exact = true
                }
                return ret != -1
        })
        if !exact && index > 0 {
                index--
        }
        c.stack[len(c.stack)-1].index = index

        // Recursively search to the next page.
        c.search(key, n.inodes[index].pgid)
}
*/
void Cursor::searchNode(bolt::bytes key, impl::node_ptr n) {
    bool exact = false;
    auto it = std::find_if(
        n->inodes.begin(), n->inodes.end(), [&](impl::inode &item) -> bool {
          auto ret = std::lexicographical_compare_three_way(
              item.key.begin(), item.key.end(), key.begin(), key.end());
          if (std::is_eq(ret)) {
              exact = true;
          }
          return !std::is_lt(ret);
        });
    auto index = std::distance(n->inodes.begin(), it);
    if (!exact && index > 0) {
        index--;
    }
    auto &e = stack.back();
    e.index = static_cast<int>(index);
    // Recursively search to the next page.
    search(key, n->inodes[index].pgid);
}

void Cursor::searchPage(bolt::bytes key, impl::page *p) {
    // Binary search for the correct range.
    auto inodes = p->branchPageElements();
    bool exact = false;
    auto it = std::find_if(inodes.begin(), inodes.end(),
                           [&](impl::branchPageElement &item) -> bool {
                             auto k = item.key();
                             auto ret = std::lexicographical_compare_three_way(
                                 k.begin(), k.end(), key.begin(), key.end());
                             if (std::is_eq(ret)) {
                                 exact = true;
                             }
                             return !std::is_lt(ret);
                           });
    auto index = std::distance(inodes.begin(), it);
    if (!exact && index > 0) {
        index--;
    }
    auto &e = stack.back();
    e.index = static_cast<int>(index);
    search(key, inodes[index].pgid);
}

void Cursor::nsearch(bolt::bytes key) {
    auto &e = stack.back();
    auto p = e.page;
    auto n = e.node;

    // If we have a node then search its inodes.
    if (!n.expired()) {
        auto nptr = n.lock();
        auto it = std::find_if(
            nptr->inodes.begin(), nptr->inodes.end(),
            [&](impl::inode &item) -> bool {
              auto ret = std::lexicographical_compare_three_way(
                  item.key.begin(), item.key.end(), key.begin(), key.end());
              return !std::is_lt(ret);
            });
        auto index = std::distance(nptr->inodes.begin(), it);
        e.index = static_cast<int>(index);
        return;
    }
    // If we have a page then search its leaf elements.
    auto inodes = p->leafPageElements();
    auto it = std::find_if(inodes.begin(), inodes.end(),
                           [&](impl::leafPageElement &item) -> bool {
                             auto k = item.key();
                             auto ret = std::lexicographical_compare_three_way(
                                 k.begin(), k.end(), key.begin(), key.end());
                             return !std::is_lt(ret);
                           });
    auto index = std::distance(inodes.begin(), it);
    e.index = static_cast<int>(index);
}

} // namespace bolt
