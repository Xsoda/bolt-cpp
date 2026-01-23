#include "impl/node.hpp"
#include "impl/db.hpp"
#include "impl/tx.hpp"
#include "impl/freelist.hpp"
#include "impl/utils.hpp"
#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <iostream>
#ifndef NDEBUG
#include <ranges>
#include "fmt/format.h"
#endif

namespace bolt::impl {

node::node(impl::BucketPtr bucket, std::initializer_list<impl::node_ptr> children) {
    this->bucket = bucket;
    this->children = children;
    isLeaf = false;
    unbalanced = false;
    spilled = false;
}

node::node(impl::BucketPtr bucket, bool isLeaf, impl::node_ptr parent) {
    this->bucket = bucket;
    this->parent = parent;
    this->isLeaf = isLeaf;
    unbalanced = false;
    spilled = false;
}

node::node(impl::BucketPtr bucket) {
    this->bucket = bucket;
    isLeaf = false;
    unbalanced = false;
    spilled = false;
}

node::node(bool isLeaf) {
    this->isLeaf = isLeaf;
    unbalanced = false;
    spilled = false;
}

impl::node_ptr node::root() {
    if (parent.expired()) {
        return shared_from_this();
    }
    return parent.lock()->root();
}

size_t node::minKeys() const {
    if (isLeaf) {
        return 1;
    }
    return 2;
}

size_t node::size() const {
    size_t sz = impl::pageHeaderSize;
    size_t elsz = pageElementSize();
    for (auto &it : inodes) {
        sz += elsz + it.key.size() + it.value.size();
    }
    return sz;
}

bool node::sizeLessThan(size_t v) const {
    size_t sz = impl::pageHeaderSize;
    size_t elsz = pageElementSize();
    for (size_t i = 0; i < inodes.size(); i++) {
        auto &item = inodes[i];
        sz += elsz + item.key.size() + item.value.size();
        if (sz >= v) {
                return false;
        }
    }
    return true;
}

size_t node::pageElementSize() const {
    if (isLeaf) {
        return impl::leafPageElementSize;
    }
    return impl::branchPageElementSize;
}

impl::node_ptr node::childAt(ptrdiff_t index) {
    if (isLeaf) {
        _assert(false, "invalid chatAt({}) on a leaf node", index);
    }
    if (bucket.expired()) {
        _assert(false, "bucket pointer already expired");
    }
    auto ptr = bucket.lock();
    return ptr->node(inodes[index].pgid, shared_from_this());
}

ptrdiff_t node::childIndex(impl::node_ptr child) {
    auto it =
        std::find_if(inodes.begin(), inodes.end(), [&](impl::inode &n) -> bool {
          auto ret = std::lexicographical_compare_three_way(std::begin(child->key), std::end(child->key),
                                std::begin(n.key), std::end(n.key));
          return !std::is_lt(ret);
    });
    return std::distance(inodes.begin(), it);
}

size_t node::numChildren() const {
    return inodes.size();
}

impl::node_ptr node::nextSibling() {
    if (parent.expired()) {
        return nullptr;
    }
    auto pptr = parent.lock();
    size_t index = (size_t)pptr->childIndex(shared_from_this());
    if (index >= pptr->numChildren() - 1) {
        return nullptr;
    }
    return pptr->childAt(index + 1);
}

impl::node_ptr node::prevSibling() {
    if (parent.expired()) {
        return nullptr;
    }
    auto pptr = parent.lock();
    size_t index = (size_t)pptr->childIndex(shared_from_this());
    if (index >= pptr->numChildren() - 1) {
        return nullptr;
    }
    return pptr->childAt(index - 1);
}

void node::put(bolt::bytes oldKey, bolt::bytes newKey, bolt::bytes value,
    impl::pgid pgid, std::uint32_t flags) {
    auto bptr = bucket.lock();
    if (!bptr) {
        _assert(false, "bucket invalid");
    }
    auto tptr = bptr->tx.lock();
    if (!tptr) {
        _assert(false, "tx invalid");
    }
    if (pgid >= tptr->meta.pgid) {
        _assert(false, "pgid ({}) above high water mark ({})", pgid, tptr->meta.pgid);
    }
    else if (oldKey.size() <= 0) {
        _assert(false, "put: zero-length old key");
    }
    else if (newKey.size() <= 0) {
        _assert(false, "put: zero-length new key");
    }

    // Find insertion index.
    auto it = std::find_if(
        inodes.begin(), inodes.end(), [&](impl::inode& item) -> bool {
            auto ret = std::lexicographical_compare_three_way(
                item.key.begin(), item.key.end(), oldKey.begin(), oldKey.end());
            return !std::is_lt(ret);
        });
    auto index = (size_t)std::distance(inodes.begin(), it);

    // Add capacity and shift nodes if we don't have an exact match and need to
    // insert.
    auto exact = inodes.size() > 0 && index < inodes.size() &&
        std::is_eq(std::lexicographical_compare_three_way(inodes[index].key.begin(),
                                                          inodes[index].key.end(),
                                                          oldKey.begin(), oldKey.end()));
    if (!exact) {
        inodes.insert(inodes.begin() + index, impl::inode{});
    }
    impl::inode &inode = inodes[index];
    inode.flags = flags;

    inode.memory.clear();
    inode.memory.reserve(newKey.size() + value.size());

    std::copy(newKey.begin(), newKey.end(), std::back_inserter(inode.memory));
    inode.key = bolt::bytes(inode.memory.begin(), inode.memory.end());

    std::copy(value.begin(), value.end(), std::back_inserter(inode.memory));
    inode.value = bolt::bytes(inode.memory.begin() + newKey.size(), inode.memory.end());

    inode.pgid = pgid;
    _assert(inode.key.size() > 0, "put: zero-length inode key");
}

void node::del(bolt::bytes key) {
    auto it = std::find_if(
        inodes.begin(), inodes.end(), [&](impl::inode &item) -> bool {
          auto ret = std::lexicographical_compare_three_way(
              key.begin(), key.end(), item.key.begin(), item.key.end());
          return !std::is_lt(ret);
    });
    if (it == inodes.end()) {
        return;
    }
    inodes.erase(it);
    unbalanced = true;
}

void node::read(impl::page *p) {
    pgid = p->id;
    isLeaf = (p->flags & impl::leafPageFlag) != 0;
    inodes.resize(p->count);
    for (int i = 0; i < p->count; i++) {
        impl::inode &inode = inodes[i];
        if (isLeaf) {
            impl::leafPageElement *elem = p->leafPageElement(i);
            inode.flags = elem->flags;
            inode.key = elem->key();
            inode.value = elem->value();
        } else {
            impl::branchPageElement *elem = p->branchPageElement(i);
            inode.pgid = elem->pgid;
            inode.key = elem->key();
        }
        _assert(inode.key.size() > 0, "read: zero-length inode key");
    }
    if (inodes.size() > 0) {
        this->key = inodes[0].key;
        _assert(this->key.size() > 0, "read: zero-length node key");
    } else {
        this->key = bolt::bytes();
    }
}

void node::write(impl::page *p) {
    // Initialize page.
    if (isLeaf) {
        p->flags |= impl::leafPageFlag;
    } else {
        p->flags |= impl::branchPageFlag;
    }

    if (inodes.size() > 0xFFFF) {
        size_t size = inodes.size();
        _assert(false, "inode overflow: {} (pgid={})", size, p->id);
    }
    p->count = (uint16_t)inodes.size();

    // Stop here if there are no items to write.
    if (p->count == 0) {
        return;
    }

    // Loop over each item and write it to the page.
    std::byte *buf = &reinterpret_cast<std::byte *>(
        &p->ptr)[pageElementSize() * inodes.size()];
    for (size_t i = 0; i < inodes.size(); i++) {
        auto item = inodes[i];
        _assert(item.key.size() > 0, "write: zero-length inode key");
        // Write the page element.
        if (isLeaf) {
            auto elem = p->leafPageElement((uint16_t)i);
            elem->pos = (uint32_t)(buf - reinterpret_cast<std::byte*>(elem));
            elem->flags = item.flags;
            elem->ksize = (uint32_t)item.key.size();
            elem->vsize = (uint32_t)item.value.size();
        } else {
            auto elem = p->branchPageElement((uint16_t)i);
            elem->pos = (uint32_t)(buf - reinterpret_cast<std::byte*>(elem));
            elem->ksize = (uint32_t)item.key.size();
            elem->pgid = item.pgid;
            _assert(elem->pgid != p->id , "write: circular dependency occurred");
        }

        std::memcpy(buf, item.key.data(), item.key.size());
        buf += item.key.size();
        std::memcpy(buf, item.value.data(), item.value.size());
        buf += item.value.size();
    }
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
std::tuple<impl::node_ptr, impl::node_ptr> node::splitTwo(size_t pageSize) {
    // Ignore the split if the page doesn't have at least enough nodes for
    // two pages or if the nodes can fit in a single page.
    if (inodes.size() <= impl::minKeysPerPage * 2
        || sizeLessThan(pageSize)) {
        return std::make_tuple(shared_from_this(), nullptr);
    }
    auto bptr = bucket.lock();
    auto tptr = bptr->tx.lock();
    // Determine the threshold before starting a new node.
    auto fillPercent = bptr->FillPercent;
    if (fillPercent < minFillPercent) {
        fillPercent = minFillPercent;
    } else if (fillPercent > maxFillPercent) {
        fillPercent = maxFillPercent;
    }
    size_t threshold = (size_t)(pageSize * fillPercent);
    size_t splitIdx;

    // Determine split position and sizes of the two pages.
    std::tie(splitIdx, std::ignore) = splitIndex(threshold);

    // Split node into two separate nodes.
    // If there's no parent then we'll need to create one.
    auto pptr = parent.lock();
    if (!pptr) {
        pptr = std::make_shared<node>(
            bptr, std::initializer_list<impl::node_ptr>({shared_from_this()}));
    }
    parent = pptr;

    // Create a new node and add it to the parent.
    auto next = std::make_shared<node>(bptr, isLeaf, pptr);
    pptr->children.push_back(next);

    // Split inodes across two nodes.
    std::copy(std::next(inodes.begin(), splitIdx), inodes.end(),
              std::back_inserter(next->inodes));
    inodes.erase(std::next(inodes.begin(), splitIdx), inodes.end());

    // NOTE: only for manage parent shared_pointer: TestNode_split()
    // golang version not cantain this code
    if (!bptr->rootNode) {
        bptr->rootNode = pptr;
    }

    tptr->stats.Split++;
    return std::make_tuple(shared_from_this(), next);
}

std::tuple<size_t, size_t> node::splitIndex(size_t threshold) {
    size_t index;
    size_t sz = impl::pageHeaderSize;
    for (size_t i = 0; i < inodes.size() - impl::minKeysPerPage; i++) {
        index = i;
        impl::inode &inode = inodes.at(i);
        size_t elsize = pageElementSize() + inode.key.size() + inode.value.size();

        if (i >= impl::minKeysPerPage && sz + elsize > threshold) {
            break;
        }
        sz += elsize;

    }
    return std::make_tuple(index, sz);
}

// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
bolt::ErrorCode node::spill() {
    auto bptr = bucket.lock();
    auto tptr = bptr->tx.lock();
    auto dbptr = tptr->db.lock();
    if (spilled) {
        return bolt::ErrorCode::Success;
    }

    // Spill child nodes first. Child nodes can meterialize sibling nodes in
    // the case of split-merge so we cannot use a range loop. We have to check
    // the children size on every loop iteration
    std::sort(children.begin(), children.end(),
              [](impl::node_ptr a, impl::node_ptr b) -> bool {
                  auto ret = std::lexicographical_compare_three_way(a->key.begin(), a->key.end(), b->key.begin(), b->key.end());
                  return std::is_lt(ret);
              });
    for (size_t i = 0; i < children.size(); i++) {
        auto err = children[i]->spill();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }

    // We no longger need the child list because it's only used for spill tracking.
    children.clear();

    // Split nodes into appropriate sizes. The first node will always be n.
    auto nodes = split(dbptr->pageSize);
    for (auto it : nodes) {
        // Add node's page to the freelist if it's not new.
        if (it->pgid > 0) {
            dbptr->freelist->free(tptr->meta.txid, tptr->page(it->pgid));
            it->pgid = 0;
        }

        // Allocate contiguous space for the node.
        auto [p, err] = tptr->allocate((it->size() / dbptr->pageSize) + 1);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }

        // Write the node.
        if (p->id >= tptr->meta.pgid) {
            _assert(false, "pgid ({}) above high water mark ({})", p->id, tptr->meta.pgid);
        }
        it->pgid = p->id;
        it->write(p);
        it->spilled = true;

        // Insert into parent inodes.
        if (auto pptr = it->parent.lock()) {
            auto k = it->key;
            if (k.empty()) {
                k = it->inodes[0].key;
            }
            pptr->put(key, it->inodes[0].key, bolt::bytes(), it->pgid, 0);
            it->key = it->inodes[0].key;
            _assert(it->key.size() > 0, "spill: zero-length node key");
        }
        // Update the statistics.
        tptr->stats.Spill++;
    }

    // If the root node split and created a new root then we need to spill that
    // as well. We'll clear out the children to make sure it doesn't try to
    // respill
    if (auto pptr = parent.lock()) {
        if (pptr->pgid == 0) {
            children.clear();
            return pptr->spill();
        }
    }
    return bolt::ErrorCode::Success;
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
void node::rebalance() {
    if (!unbalanced) {
        return;
    }
    auto bptr = bucket.lock();
    auto tptr = bptr->tx.lock();
    auto dbptr = tptr->db.lock();
    unbalanced = false;

    // Update statistics.
    tptr->stats.Rebalance++;

    // Ignore if node is above threshold (25%) and has enough keys.
    int threshold = dbptr->pageSize / 4;
    if (size() > threshold && inodes.size() > (size_t)minKeys()) {
        return;
    }

    // Root node has special handling.
    if (parent.expired()) {
        // If root node is a branch and only has one node then collapse it.
        if (isLeaf && inodes.size() == 1) {
            // Move root's child up.
            impl::node_ptr child = bptr->node(inodes.front().pgid, shared_from_this());
            isLeaf = child->isLeaf;
            inodes = child->inodes;
            children = child->children;

            // Reparent all child nodes being moved.
            for (auto &it : inodes) {
                auto item = bptr->nodes.find(it.pgid);
                if (item != bptr->nodes.end()) {
                    item->second->parent = shared_from_this();
                }
            }

            // Remove old child.
            child->parent.reset();
            auto it = bptr->nodes.find(child->pgid);
            if (it != bptr->nodes.end()) {
                bptr->nodes.erase(it);
            }
            child->free();
        }
        return;
    }

    // If node has no keys then just remove it.
    auto pptr = parent.lock();
    if (numChildren() == 0) {
        pptr->del(key);
        pptr->removeChild(shared_from_this());
        auto it = bptr->nodes.find(pgid);
        if (it != bptr->nodes.end()) {
            bptr->nodes.erase(it);
        }
        this->free();
        pptr->rebalance();
        return;
    }

    _assert(pptr->numChildren() > 1, "parent must have at least 2 children");

    // Destination node is right sibling if idx == 0, otherwise left sibling.
    impl::node_ptr target;
    bool useNextSibling = pptr->childIndex(shared_from_this()) == 0;
    if (useNextSibling) {
        target = nextSibling();
    } else {
        target = prevSibling();
    }

    // If both this node and the target node are too small then merge them.
    if (useNextSibling) {
        // Reparent all child nodes being moved.
        for (auto &item : target->inodes) {
            auto it = bptr->nodes.find(item.pgid);
            if (it != bptr->nodes.end()) {
                impl::node_ptr child = it->second;
                auto cp = child->parent.lock();
                cp->removeChild(child);

                child->parent = shared_from_this();
                children.push_back(child);
            }
        }

        // Copy over inodes from target and remove target.
        std::copy(target->inodes.begin(), target->inodes.end(),
                  std::back_inserter(inodes));
        pptr->del(target->key);
        pptr->removeChild(target);
        auto it = bptr->nodes.find(target->pgid);
        if (it != bptr->nodes.end()) {
            bptr->nodes.erase(it);
        }
        target->free();
    } else {
        // Reparent all child nodes being moved.
        for (auto &item : inodes) {
            auto it = bptr->nodes.find(item.pgid);
            if (it != bptr->nodes.end()) {
                impl::node_ptr child = it->second;
                auto cp = child->parent.lock();
                cp->removeChild(child);

                child->parent = target;
                target->children.push_back(child);
            }
        }

        // Copy over inodes to target and remove node.
        std::copy(inodes.begin(), inodes.end(),
                  std::back_inserter(target->inodes));
        pptr->del(key);
        pptr->removeChild(shared_from_this());
        auto it = bptr->nodes.find(pgid);
        if (it != bptr->nodes.end()) {
            bptr->nodes.erase(it);
        }
        free();
    }

    // Either this node or the target node was deleted from the parent so
    // rebalance it.
    pptr->rebalance();
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
void node::removeChild(impl::node_ptr target) {
    std::ignore =
        std::remove_if(children.begin(), children.end(),
                       [&](impl::node_ptr item) { return item == target; });
}

// dereference causes the node to copy all its inode key/value references to
// heap memory. This is required when the mmap is reallocated so inodes are not
// pointing to stale data.
void node::dereference() {
    if (key.size() > 0) {
        memory.clear();
        memory.reserve(key.size());
        std::copy(key.begin(), key.end(), std::back_inserter(memory));
        key = bolt::bytes(memory.begin(), memory.end());
        _assert(pgid == 0 || key.size() > 0, "dereference: zero-length node key on existing node");
    }
    for (auto &it : inodes) {
        std::vector<std::byte> key, value;
        key.assign(it.key.begin(), it.key.end());
        value.assign(it.value.begin(), it.value.end());
        _assert(key.size() > 0, "dereference: zero-length inode key");
        it.memory.clear();
        it.memory.reserve(key.size() + value.size());

        std::copy(key.begin(), key.end(), std::back_inserter(it.memory));
        it.key = bolt::bytes(it.memory.begin(), it.memory.end());

        std::copy(value.begin(), value.end(), std::back_inserter(it.memory));
        it.value = bolt::bytes(it.memory.begin() + key.size(), it.memory.end());
    }

    // Recursively dereference children.
    for (auto it : children) {
        it->dereference();
    }
    auto bptr = bucket.lock();
    auto tptr = bptr->tx.lock();
    tptr->stats.NodeDeref++;
}

void node::free() {
    if (pgid != 0) {
        auto bptr = bucket.lock();
        auto tptr = bptr->tx.lock();
        auto dbptr = tptr->db.lock();
        dbptr->freelist->free(tptr->meta.txid, tptr->page(pgid));
        pgid = 0;
    }
}

std::vector<impl::node_ptr> node::split(size_t pageSize) {
    std::vector<impl::node_ptr> nodes;
    auto node = shared_from_this();
    while (true) {
        // Split node into two.
        auto [a, b] = node->splitTwo(pageSize);
        nodes.push_back(a);

        // If we can't split then exit the loop.
        if (b == nullptr) {
            break;
        }

        // Set node to b so it gets split on the next iteration.
        node = b;
    }
    return nodes;
}

// dump writes the contents of the node to STDOUT for debugging purposes.
void node::dump() {
#ifndef NDEBUG
    std::string type = "branch";
    if (isLeaf) {
        type = "leaf";
    }
    auto trunc = [](bolt::bytes val, size_t len) -> std::string {
      auto vec =
          std::views::all(val) | std::views::take(len) |
          std::views::transform([](std::byte b) -> char { return (char)b; });
      return std::string(vec.begin(), vec.end());
    };
    fmt::println("[NODE {} {{type={} count={}}}]", pgid, type, inodes.size());
    for (auto &item : inodes) {
        if (isLeaf) {
            if ((item.flags & bolt::impl::bucketLeafFlag) != 0) {
                auto bucket = reinterpret_cast<impl::bucket *>(&item.value[0]);
                fmt::println("+L {} -> (bucket root={})", trunc(item.key, 8), bucket->root);
            } else {
                fmt::println("+L {} -> {}", trunc(item.key, 8), trunc(item.value,8));
            }
        } else {
            fmt::println("+B {} -> pgid={}", trunc(item.key, 8), item.pgid);
        }
    }
    fmt::println("");
#endif
}
}
