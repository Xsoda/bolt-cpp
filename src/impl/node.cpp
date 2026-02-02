#include "impl/node.hpp"
#include "impl/db.hpp"
#include "impl/tx.hpp"
#include "impl/freelist.hpp"
#include "impl/utils.hpp"
#include "impl/bsearch.hpp"
#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <iostream>
#include <span>
#ifndef NDEBUG
#include <ranges>
#include "fmt/format.h"
#endif

namespace bolt::impl {
inode::inode() : flags(0), pgid(0) {}

inode::inode(const inode &other) noexcept {
    this->flags = other.flags;
    this->pgid = other.pgid;
    this->memory = other.memory;
    this->key = bolt::bytes(this->memory.data(), other.key.size());
    this->value = bolt::bytes(this->memory.data() + other.key.size(), other.value.size());
    if (memory.empty() && key.size() > 0) {
        memory.reserve(key.size() + value.size());
        std::copy(key.begin(), key.end(), std::back_inserter(memory));
        key = bolt::bytes(memory.data(), key.size());
        std::copy(value.begin(), value.end(), std::back_inserter(memory));
        value = bolt::bytes(memory.data() + key.size(), value.size());
    }
}

inode::inode(inode &&other) noexcept {
    this->flags = other.flags;
    this->pgid = other.pgid;
    std::swap(this->memory, other.memory);
    std::swap(this->key, other.key);
    std::swap(this->value, other.value);
    if (memory.empty() && key.size() > 0) {
        memory.reserve(key.size() + value.size());
        std::copy(key.begin(), key.end(), std::back_inserter(memory));
        key = bolt::bytes(memory.data(), key.size());
        std::copy(value.begin(), value.end(), std::back_inserter(memory));
        value = bolt::bytes(memory.data() + key.size(), value.size());
    }
}

inode &inode::operator=(const inode &other) noexcept {
    this->memory = other.memory;
    this->flags = other.flags;
    this->pgid = other.pgid;
    this->key = bolt::bytes(this->memory.data(), other.key.size());
    this->value = bolt::bytes(this->memory.data() + other.key.size(), other.value.size());
    if (memory.empty() && key.size() > 0) {
        memory.reserve(key.size() + value.size());
        std::copy(key.begin(), key.end(), std::back_inserter(memory));
        key = bolt::bytes(memory.data(), key.size());
        std::copy(value.begin(), value.end(), std::back_inserter(memory));
        value = bolt::bytes(memory.data() + key.size(), value.size());
    }
    return *this;
}

inode &inode::operator=(inode &&other) noexcept {
    this->flags = other.flags;
    this->pgid = other.pgid;
    std::swap(this->memory, other.memory);
    std::swap(this->key, other.key);
    std::swap(this->value, other.value);
    if (memory.empty() && key.size() > 0) {
        memory.reserve(key.size() + value.size());
        std::copy(key.begin(), key.end(), std::back_inserter(memory));
        key = bolt::bytes(memory.data(), key.size());
        std::copy(value.begin(), value.end(), std::back_inserter(memory));
        value = bolt::bytes(memory.data() + key.size(), value.size());
    }
    return *this;
}

node::node(impl::BucketPtr bucket, std::initializer_list<impl::node_ptr> children): pgid(0) {
    this->bucket = bucket;
    this->children = children;
    isLeaf = false;
    unbalanced = false;
    spilled = false;
}

node::node(impl::BucketPtr bucket, bool isLeaf, impl::node_ptr parent)
    : pgid(0) {
    this->bucket = bucket;
    this->parent = parent;
    this->isLeaf = isLeaf;
    unbalanced = false;
    spilled = false;
}

node::node(impl::BucketPtr bucket) : pgid(0) {
    this->bucket = bucket;
    isLeaf = false;
    unbalanced = false;
    spilled = false;
}

node::node(bool isLeaf) : pgid(0) {
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

bool node::sizeLessThan(size_t v, size_t off) const {
    size_t sz = impl::pageHeaderSize;
    size_t elsz = pageElementSize();
    for (size_t i = off; i < inodes.size(); i++) {
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
    // auto it =
    //     std::find_if(inodes.begin(), inodes.end(), [&](impl::inode &n) ->
    //     bool {
    //       auto ret = std::lexicographical_compare_three_way(
    //           std::begin(n.key), std::end(n.key), std::begin(child->key),
    //           std::end(child->key));
    //       return !std::is_lt(ret);
    //     });
    auto [it, cmp] =
        impl::bsearch(std::begin(inodes), std::end(inodes), child,
                      [](const impl::node_ptr &child,
                         impl::inode &n) -> std::strong_ordering {
                        return std::lexicographical_compare_three_way(
                            std::begin(child->key), std::end(child->key),
                            std::begin(n.key), std::end(n.key));
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
    // auto it = std::find_if(
    //     inodes.begin(), inodes.end(), [&](impl::inode& item) -> bool {
    //         auto ret = std::lexicographical_compare_three_way(
    //             item.key.begin(), item.key.end(), oldKey.begin(), oldKey.end());
    //         return !std::is_lt(ret);
    //     });
    // auto index = (size_t)std::distance(inodes.begin(), it);

    // Add capacity and shift nodes if we don't have an exact match and need to
    // insert.
    // auto exact = inodes.size() > 0 && index < inodes.size() &&
    //     std::is_eq(std::lexicographical_compare_three_way(inodes[index].key.begin(),
    //                                                       inodes[index].key.end(),
    //                                                       oldKey.begin(),
    //                                                       oldKey.end()));
    auto [it, cmp] = impl::bsearch(
        std::begin(inodes), std::end(inodes), oldKey,
        [](const bolt::bytes &key, impl::inode &item) -> std::strong_ordering {
          return std::lexicographical_compare_three_way(
              std::begin(key), std::end(key), std::begin(item.key),
              std::end(item.key));
        });
    auto index = std::distance(std::begin(inodes), it);
    auto exact = inodes.size() > 0 && index < inodes.size() && std::is_eq(cmp);
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

// del removes a key from the node.
void node::del(bolt::bytes k) {
    // Find index of key.
    // auto it = std::find_if(
    //     inodes.begin(), inodes.end(), [&](impl::inode &item) -> bool {
    //       auto ret = std::lexicographical_compare_three_way(
    //           item.key.begin(), item.key.end(), k.begin(), k.end());
    //       return !std::is_lt(ret);
    //     });
    // // Exit if the key isn't found.
    // if (it == inodes.end() ||
    //     !std::is_eq(std::lexicographical_compare_three_way(
    //         it->key.begin(), it->key.end(), k.begin(), k.end()))) {
    //     log_debug("### node {} del [{}, {}] not found", pgid, k, it->key);
    //     dump();
    //     return;
    // }
    auto [it, cmp] = impl::bsearch(
        std::begin(inodes), std::end(inodes), k,
        [](const bolt::bytes &k, impl::inode &item) -> std::strong_ordering {
          return std::lexicographical_compare_three_way(
              std::begin(k), std::end(k), std::begin(item.key),
              std::end(item.key));
        });
    if (it == inodes.end() || !std::is_eq(cmp)) {
        log_debug("### node {} del [{}, {}] not found", pgid, k, it->key);
        dump();
        return;
    }
    // Delete inode from the node.
    inodes.erase(it);
    // Mark the node as needing rebalancing.
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
        auto &item = inodes[i];
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
std::tuple<impl::node_ptr, impl::node_ptr>
node::splitTwo(size_t pageSize, std::vector<impl::node_ptr> &hold) {
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

        hold.push_back(pptr); // hold std::shared_ptr
    }
    parent = pptr;

    // Create a new node and add it to the parent.
    auto next = std::make_shared<node>(bptr, isLeaf, pptr);
    pptr->children.push_back(next);

    // Split inodes across two nodes.
    std::move(std::next(inodes.begin(), splitIdx), inodes.end(),
              std::back_inserter(next->inodes));
    inodes.erase(std::next(inodes.begin(), splitIdx), inodes.end());

    tptr->stats.Split++;
    return std::make_tuple(shared_from_this(), next);
}

std::tuple<size_t, size_t> node::splitIndex(size_t threshold, size_t off) {
    size_t index;
    size_t sz = impl::pageHeaderSize;
    for (size_t i = off; i < inodes.size() - impl::minKeysPerPage; i++) {
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
bolt::ErrorCode node::spill(std::vector<impl::node_ptr> &hold) {
    auto bktptr = bucket.lock();
    auto txptr = bktptr->tx.lock();
    auto dbptr = txptr->db.lock();
    if (spilled) {
        return bolt::ErrorCode::Success;
    }
    log_debug("1. spill node {}", pgid);
    // Spill child nodes first. Child nodes can meterialize sibling nodes in
    // the case of split-merge so we cannot use a range loop. We have to check
    // the children size on every loop iteration
    std::sort(children.begin(), children.end(),
              [](impl::node_ptr &a, impl::node_ptr &b) -> bool {
                auto ret = std::lexicographical_compare_three_way(
                    a->key.begin(), a->key.end(), b->key.begin(), b->key.end());
                return std::is_lt(ret);
              });
    log_debug("node {} sort complete", pgid);
    for (size_t i = 0; i < children.size(); i++) {
        auto err = children[i]->spill(hold);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }

    // We no longger need the child list because it's only used for spill tracking.
    children.clear();

    // Split nodes into appropriate sizes. The first node will always be n.
    auto nodes = split_v2(dbptr->pageSize, hold);

    for (auto &it : nodes) {
        // Add node's page to the freelist if it's not new.
        if (it->pgid > 0) {
            dbptr->freelist->free(txptr->meta.txid, txptr->page(it->pgid));
            it->pgid = 0;
        }

        // Allocate contiguous space for the node.
        auto [p, err] = txptr->allocate((it->size() / dbptr->pageSize) + 1);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }

        // Write the node.
        if (p->id >= txptr->meta.pgid) {
            _assert(false, "pgid ({}) above high water mark ({})", p->id, txptr->meta.pgid);
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
            pptr->put(k, it->inodes[0].key, bolt::bytes(), it->pgid, 0);
            it->key = it->inodes[0].key;
            _assert(it->key.size() > 0, "spill: zero-length node key");
        }
        // Update the statistics.
        txptr->stats.Spill++;
    }

    // If the root node split and created a new root then we need to spill that
    // as well. We'll clear out the children to make sure it doesn't try to
    // respill
    if (auto pptr = parent.lock()) {
        if (pptr->pgid == 0) {
            children.clear();
            return pptr->spill(hold);
        }
    }
    log_debug("2. spill node {}", pgid);
    return bolt::ErrorCode::Success;
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
void node::rebalance() {
    if (!unbalanced) {
        log_debug("*** node {} already rebalanced", pgid);
        return;
    }
    auto bktptr = bucket.lock();
    auto txptr = bktptr->tx.lock();
    auto dbptr = txptr->db.lock();
    unbalanced = false;

    // Update statistics.
    txptr->stats.Rebalance++;

    // Ignore if node is above threshold (25%) and has enough keys.
    int threshold = dbptr->pageSize / 4;
    if (size() > threshold && inodes.size() > (size_t)minKeys()) {
        log_debug("*** node {} unneed rebalance", pgid);
        return;
    }
    log_debug("*** rebalance node {}", pgid);
    // Root node has special handling.
    if (parent.expired()) {
        // If root node is a branch and only has one node then collapse it.
        if (!isLeaf && inodes.size() == 1) {
            // Move root's child up.
            impl::node_ptr child =
                bktptr->node(inodes.front().pgid, shared_from_this());
            isLeaf = child->isLeaf;
            inodes = child->inodes;
            children = child->children;

            // Reparent all child nodes being moved.
            for (auto &it : inodes) {
                auto item = bktptr->nodes.find(it.pgid);
                if (item != bktptr->nodes.end()) {
                    item->second->parent = shared_from_this();
                }
            }

            // Remove old child.
            child->parent.reset();
            auto it = bktptr->nodes.find(child->pgid);
            if (it != bktptr->nodes.end()) {
                bktptr->nodes.erase(it);
            }
            child->free();
        }
        return;
    }

    // If node has no keys then just remove it.
    auto pptr = parent.lock();
    if (numChildren() == 0) {
        impl::node_ptr self = shared_from_this();
        // log_debug("# - 0 node {} del {} - {}", pptr->pgid, pgid, key);
        pptr->del(key);
        pptr->removeChild(self);
        auto it = bktptr->nodes.find(pgid);
        if (it != bktptr->nodes.end()) {
            bktptr->nodes.erase(it);
        }
        self->free();
        auto ppid = pptr->pgid;
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
            if (auto it = bktptr->nodes.find(item.pgid);
                it != bktptr->nodes.end()) {
                impl::node_ptr child = it->second;
                auto cp = child->parent.lock();
                cp->removeChild(child);

                child->parent = shared_from_this();
                children.push_back(child);
            }
        }

        // Copy over inodes from target and remove target.
        std::move(target->inodes.begin(), target->inodes.end(),
                  std::back_inserter(inodes));
        // log_debug("# - 1 node {} del {} - {}", pptr->pgid, target->pgid, target->key);
        pptr->del(target->key);
        pptr->removeChild(target);
        auto it = bktptr->nodes.find(target->pgid);
        if (it != bktptr->nodes.end()) {
            bktptr->nodes.erase(it);
        }
        target->free();
    } else {
        // Reparent all child nodes being moved.
        for (auto &item : inodes) {
            auto it = bktptr->nodes.find(item.pgid);
            if (it != bktptr->nodes.end()) {
                impl::node_ptr child = it->second;
                auto cp = child->parent.lock();
                cp->removeChild(child);

                child->parent = target;
                target->children.push_back(child);
            } else {
                log_debug("* bucket node {} not found", item.pgid);
            }
        }

        // Copy over inodes to target and remove node.
        impl::node_ptr self = shared_from_this();
        std::move(inodes.begin(), inodes.end(),
                  std::back_inserter(target->inodes));
        // log_debug("# - 2 node {} del {} - {}", pptr->pgid, pgid, key);
        pptr->del(key);
        pptr->removeChild(self);
        auto it = bktptr->nodes.find(pgid);
        if (it != bktptr->nodes.end()) {
            bktptr->nodes.erase(it);
        } else {
            log_debug("* bucket remove node {} not found", pgid);
        }
        self->free();
    }
    // Either this node or the target node was deleted from the parent so
    // rebalance it.
    pptr->rebalance();
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
void node::removeChild(impl::node_ptr target) {
    auto it =
        std::remove_if(children.begin(), children.end(),
                       [&](impl::node_ptr item) { return item == target; });
    if (it != children.end()) {
        children.erase(it);
    } else {
        log_debug("{} removeChild {} not found", pgid, target->key);
        dump();
    }
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
        _assert(it.key.size() > 0, "dereference: zero-length inode key");
        it.memory.clear();
        it.memory.reserve(it.key.size() + it.value.size());

        std::copy(it.key.begin(), it.key.end(), std::back_inserter(it.memory));
        it.key = bolt::bytes(it.memory.begin(), it.memory.end());

        std::copy(it.value.begin(), it.value.end(), std::back_inserter(it.memory));
        it.value = bolt::bytes(it.memory.begin() + it.key.size(), it.memory.end());
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

std::vector<impl::node_ptr> node::split(size_t pageSize, std::vector<impl::node_ptr> &hold) {
    std::vector<impl::node_ptr> nodes;
    auto node = shared_from_this();
    while (true) {
        // Split node into two.
        auto [a, b] = node->splitTwo(pageSize, hold);
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

std::vector<impl::node_ptr> node::split_v2(size_t pageSize,
                                           std::vector<impl::node_ptr> &hold) {
    std::vector<impl::node_ptr> nodes;
    std::vector<std::span<impl::inode>> split_result;
    auto bktptr = bucket.lock();
    auto txptr = bktptr->tx.lock();
    // Determine the threshold before starting a new node.
    auto fillPercent = bktptr->FillPercent;
    if (fillPercent < minFillPercent) {
        fillPercent = minFillPercent;
    } else if (fillPercent > maxFillPercent) {
        fillPercent = maxFillPercent;
    }
    size_t threshold = (size_t)(pageSize * fillPercent);
    size_t splitIdx, offset = 0;

    while (offset < inodes.size()) {
        if (inodes.size() - offset <= impl::minKeysPerPage * 2 ||
            sizeLessThan(pageSize, offset)) {

            split_result.emplace_back(std::span<impl::inode>(
                inodes.begin() + offset, inodes.size() - offset));
            break;
        }
        std::tie(splitIdx, std::ignore) = splitIndex(threshold, offset);
        split_result.emplace_back(
            std::span<impl::inode>(inodes.begin() + offset, splitIdx - offset));
        offset = splitIdx;
    }

    if (split_result.size() == 1) {
        nodes.emplace_back(shared_from_this());
        return nodes;
    }

    auto pptr = parent.lock();
    if (!pptr) {
        pptr = std::make_shared<impl::node>(bktptr, std::initializer_list<impl::node_ptr>({shared_from_this()}));
        hold.push_back(pptr);
    }
    parent = pptr;
    nodes.assign(split_result.size(), nullptr);
    nodes[0] = shared_from_this();
    for (int i = 1; i < split_result.size(); i++) {
        nodes[i] = std::make_shared<impl::node>(bktptr, isLeaf, pptr);
        std::move(split_result[i].begin(), split_result[i].end(),
                  std::back_inserter(nodes[i]->inodes));
        pptr->children.emplace_back(nodes[i]);
        txptr->stats.Split++;
    }
    inodes.erase(std::next(inodes.begin(), split_result[0].size()), inodes.end());
    return nodes;
}

// dump writes the contents of the node to STDOUT for debugging purposes.
void node::dump() {
#ifndef NDEBUG
    std::string type = "branch";
    if (isLeaf) {
        type = "leaf";
    }
    log_debug("[NODE {} {{type={} count={}}}]", pgid, type, inodes.size());
    for (auto &item : inodes) {
        if (isLeaf) {
            if ((item.flags & bolt::impl::bucketLeafFlag) != 0) {
                auto bucket = reinterpret_cast<impl::bucket *>(&item.value[0]);
                log_debug("+L {} -> (bucket root={})", item.key, bucket->root);
            } else {
                log_debug("+L {} -> {}", item.key, item.value);
            }
        } else {
            log_debug("+B {} -> pgid={}", item.key, item.pgid);
        }
    }
    log_debug("");
#endif
}
}
