#include "cursor.hpp"
#include "common.hpp"
#include "node.hpp"
#include "page.hpp"
#include <algorithm>
#include <cassert>

namespace bolt {

bool elemRef::isLeaf() const {
    if (auto n = node.lock()) {
        return n->isLeaf;
    }
    return (this->page->flags & bolt::leafPageFlag) != 0;
}

int elemRef::count() const {
    if (auto n = node.lock()) {
        return n->inodes.size();
    }
    return this->page->count;
}

bolt::node_ptr Cursor::node() const {
    size_t len = stack.size();
    assert(len > 0);
    auto &ref = stack.back();
    if (auto n = ref.node.lock()) {
        if (ref.isLeaf()) {
            return n->shared_from_this();
        }
    }
    auto &f = stack.front();
    assert("bucket already expired" && !bucket.expired());
    auto b = bucket.lock();
    auto n = stack.front().node.lock();
    if (!n) {
        n = b->node(f.page->id, nullptr);
    }
    for (size_t i = 0; i < len - 1; i++) {
        auto ref = stack.at(i);
        assert(!n->isLeaf);
        n = n->childAt(ref.index);
    }
    assert(n->isLeaf);
    return n->shared_from_this();
}

// First moves the cursor to the first item in the bucket and returns its key
// and value. If the bucket is empty then a nil key and value are returned. The
// returned key and value are only valid for the life of the transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::First() {
    assert(!bucket.expired());
    auto bptr = bucket.lock();
    assert("tx closed" && !bptr->tx.expired());
    stack.clear();

    auto [p, n] = bptr->pageNode(bptr->bucket.root);
    stack.push_back(elemRef(p, n, 0));
    first();

    // If we land on an empty page then move to the next value.
    // https://github.com/boltdb/bolt/issues/450
    if (stack.back().count() == 0) {
        next();
    }

    auto [k, v, flags] = keyValue();
    if ((flags & bolt::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

// Next moves the cursor to the next item in the bucket and returns its key and
// value. If the cursor is at the end of the bucket then a nil key and value are
// returned. The returned key and value are only valid for the life of the
// transaction.
std::tuple<bolt::bytes, bolt::bytes> Cursor::Next() {
    assert(!bucket.expired());
    auto bptr = bucket.lock();
    assert("tx closed" && !bptr->tx.expired());

    auto [k, v, flags] = next();
    if ((flags & bolt::bucketLeafFlag) != 0) {
        return std::make_tuple(k, bolt::bytes());
    }
    return std::make_tuple(k, v);
}

std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> Cursor::keyValue() {
    auto ref = stack.back();
    if (ref.count() == 0 || ref.index >= ref.count()) {
        return std::make_tuple<bolt::bytes, bolt::bytes, std::uint32_t>(bolt::bytes(), bolt::bytes(), 0);
    }
    if (auto n = ref.node.lock()) {
        auto inode = n->inodes.at(ref.index);
        return std::make_tuple(inode.key, inode.value, inode.flags);
    }

    bolt::leafPageElement *elem = ref.page->leafPageElement(ref.index);
    return std::make_tuple(elem->key(), elem->value(), elem->flags);
}

std::tuple<bolt::bytes, bolt::bytes, std::uint32_t>
Cursor::seek(bolt::bytes k) {
    assert("Bucket already expired in Cursor" && bucket.expired());
    auto b = bucket.lock();
    assert("tx closed" && b->tx.expired());
    stack.clear();
    search(k, b->bucket.root);
    auto ref = stack.back();
    if (ref.index >= ref.count()) {
        return std::make_tuple(bolt::bytes(), bolt::bytes(), 0);
    }
    return keyValue();
}

// first moves the cursor to the first leaf element under the last page in the
// stack.
void Cursor::first() {
    while (true) {
        // Exit when we hit a leaf page.
        auto ref = stack.back();
        if (ref.isLeaf()) {
            break;
        }

        // Keep adding pages pointing to the first element to the stack.
        bolt::pgid pgid;
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
        auto ref = stack.back();
        if (ref.isLeaf()) {
            break;
        }

        // Keep adding pages pointing to the last element in the stack.
        bolt::pgid pgid;
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
        auto ref = stack.back();
        if (ref.count() == 0) {
            continue;
        }
        return keyValue();
    }
}

void Cursor::search(bolt::bytes key, bolt::pgid pgid) {
    auto b = bucket.lock();
    auto [p, n] = b->pageNode(pgid);
    if (p != nullptr && (p->flags & (branchPageFlag | leafPageFlag)) == 0) {
        assert("invalid page type" && false);
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
void Cursor::searchNode(bolt::bytes key, bolt::node_ptr n) {
    bool exact = false;
    auto it = std::find_if(
        n->inodes.begin(), n->inodes.end(), [&](bolt::inode &item) -> bool {
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
    // stack[stack.size() - 1].index = index;
    auto &e = stack.back();
    e.index = index;

    // Recursively search to the next page.
    search(key, n->inodes[index].pgid);
}

void Cursor::searchPage(bolt::bytes key, bolt::page *p) {
    // Binary search for the correct range.
    auto inodes = p->branchPageElements();
    bool exact = false;
    auto it = std::find_if(inodes.begin(), inodes.end(),
                           [&](bolt::branchPageElement &item) -> bool {
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
    auto e = stack.end();
    e->index = index;
    // stack[stack.size() - 1].index = index;
    search(key, inodes[index].pgid);
}

void Cursor::nsearch(bolt::bytes key) {
    auto e = stack.back();
    auto p = e.page;
    auto n = e.node;

    // If we have a node then search its inodes.
    if (!n.expired()) {
        auto nptr = n.lock();
        auto it = std::find_if(
            nptr->inodes.begin(), nptr->inodes.end(),
            [&](bolt::inode item) -> bool {
              auto ret = std::lexicographical_compare_three_way(
                  item.key.begin(), item.key.end(), key.begin(), key.end());
              return !std::is_lt(ret);
            });
        auto index = std::distance(nptr->inodes.begin(), it);
        e.index = index;
        return;
    }

    // If we have a page then search its leaf elements.
    auto inodes = p->leafPageElements();
    auto it = std::find_if(inodes.begin(), inodes.end(),
                           [&](bolt::leafPageElement &item) -> bool {
                             auto k = item.key();
                             auto ret = std::lexicographical_compare_three_way(
                                 k.begin(), k.end(), key.begin(), key.end());
                             return !std::is_lt(ret);
                           });
    auto index = std::distance(inodes.begin(), it);
    e.index = index;
}

} // namespace bolt
