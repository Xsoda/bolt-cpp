#include "cursor.hpp"
#include "node.hpp"
#include "page.hpp"
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

void Cursor::searchNode(bolt::bytes key, bolt::node_ptr n) {
}

void Cursor::searchPage(bolt::bytes key, bolt::page *p) {}

void Cursor::nsearch(bolt::bytes key) {

}
} // namespace bolt
