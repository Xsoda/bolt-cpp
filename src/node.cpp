#include "node.hpp"
#include "db.hpp"
#include "tx.hpp"
#include "freelist.hpp"
#include <cassert>
#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <iterator>

namespace bolt {

node::node(bolt::BucketPtr bucket, std::initializer_list<bolt::node_ptr> children) {
    this->bucket = bucket;
    this->children = children;
}

node::node(bolt::BucketPtr bucket, bool isLeaf, bolt::node_ptr parent) {
    this->bucket = bucket;
    this->parent = parent;
    this->isLeaf = isLeaf;
}


bolt::node_ptr node::root() {
    if (parent == NULL) {
        return shared_from_this();
    }
    return parent->root();
}

int node::minKeys() const {
    if (isLeaf) {
        return 1;
    }
    return 2;
}

int node::size() const {
    int sz = bolt::pageHeaderSize;
    int elsz = pageElementSize();
    for (size_t i = 0; i < inodes.size(); i++) {
        auto &item = inodes[i];
        sz += elsz + item.key.size() + item.value.size();
    }
    return sz;
}

bool node::sizeLessThan(int v) const {
    int sz = bolt::pageHeaderSize;
    int elsz = pageElementSize();
    for (size_t i = 0; i < inodes.size(); i++) {
        auto &item = inodes[i];
        sz += elsz + item.key.size() + item.value.size();
        if (sz >= v) {
                return false;
        }
    }
    return true;
}

int node::pageElementSize() const {
    if (isLeaf) {
        return bolt::leafPageElementSize;
    }
    return bolt::branchPageElementSize;
}

bolt::node_ptr node::childAt(int index) {
    if (isLeaf) {
        assert("invalid chatAt() on a leaf node" && false);
    }
    if (bucket.expired()) {
        assert("bucket pointer already invalid" && false);
    }
    auto ptr = bucket.lock();
    return ptr->node(inodes[index].pgid, shared_from_this());
}

int node::childIndex(const bolt::node_ptr child) {
    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inode &n) -> bool {
        return std::equal(std::begin(child->key), std::end(child->key),
                          std::begin(n.key), std::end(n.key));
    });
    return std::distance(inodes.begin(), it);
}

int node::numChildren() const {
    return inodes.size();
}

bolt::node_ptr node::nextSibling() const {
    if (parent == nullptr) {
        return nullptr;
    }
    int index = parent->childIndex(shared_from_this());
    if (index >= parent->numChildren() - 1) {
        return nullptr;
    }
    return parent->childAt(index + 1);
}

bolt::node_ptr node::prevSibling() const {
    if (parent == nullptr) {
        return nullptr;
    }
    int index = parent->childIndex(shared_from_this());
    if (index >= parent->numChildren() - 1) {
        return nullptr;
    }
    return parent->childAt(index - 1);
}

void node::put(bolt::bytes oldKey, bolt::bytes newKey, bolt::bytes value, bolt::pgid pgid, std::uint32_t flags) {
    if (pgid > bucket->tx->meta.pgid) {
        assert("pgid above high water mark" && false);
    } else if (oldKey.size() <= 0) {
        assert("put: zero-length old key" && false);
    } else if (newKey.size() <= 0) {
        assert("put: zero-length new key" && false);
    }

    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inode &item) -> bool {
        return std::lexicographical_compare(item.key.begin(), item.key.end(),
                                            oldKey.begin(), oldKey.end());
    });
    auto exact = inodes.size() > 0 && it != inodes.end();
    if (!exact) {
        inodes.insert(it, bolt::inode{});
    } else {
        inodes.push_back(bolt::inode{});
    }
    auto index = std::distance(inodes.begin(), it);
    bolt::inode &inode = inodes[index];
    inode.flags = flags;

    inode.memory.clear();
    inode.memory.reserve(newKey.size() + value.size());

    std::copy(newKey.begin(), newKey.end(), std::back_inserter(inode.memory));
    inode.key = bolt::bytes(inode.memory.begin(), inode.memory.end());

    std::copy(value.begin(), value.end(), std::back_inserter(inode.memory));
    inode.value = bolt::bytes(inode.memory.begin() + newKey.size(), inode.memory.end());

    inode.pgid = pgid;
    assert("put: zero-length inode key" && inode.key.size() > 0);
}

void node::del(bolt::bytes key) {
    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inode &item) -> bool {
        return std::equal(key.begin(), key.end(),
                          item.key.begin(), item.key.end());
    });
    if (it == inodes.end()) {
        return;
    }
    inodes.erase(it);
    unbalanced = true;
}

void node::read(bolt::page *p) {
    pgid = p->id;
    isLeaf = (p->flags & bolt::leafPageFlag) != 0;
    inodes.reserve(p->count);
    for (int i = 0; i < p->count; i++) {
        bolt::inode &inode = inodes[i];
        if (isLeaf) {
            bolt::leafPageElement *elem = p->leafPageElement(i);
            inode.flags = elem->flags;
            inode.key = elem->key();
            inode.value = elem->value();
        } else {
            bolt::branchPageElement *elem = p->branchPageElement(i);
            inode.pgid = elem->pgid;
            inode.key = elem->key();
        }
        assert("read: zero-length inode key" && inode.key.size() > 0);
    }
    if (inodes.size() > 0) {
        this->key = inodes[0].key;
    }
}

void node::write(bolt::page *p) {
    if (isLeaf) {
        p->flags |= bolt::leafPageFlag;
    } else {
        p->flags |= bolt::branchPageFlag;
    }

    if (inodes.size() > 0xFFFF) {
        assert("inode overflow" && 0);
    }
    p->count = inodes.size();

    if (p->count == 0) {
        return;
    }

    std::byte *buf = &reinterpret_cast<std::byte*>(&p->ptr)[pageElementSize() * inodes.size()];
    for (size_t i = 0; i < inodes.size(); i++) {
        auto item = inodes[i];
        assert("write: zero-length inode key" && item.key.size() > 0);
        if (isLeaf) {
            auto elem = p->leafPageElement(i);
            elem->pos = buf - reinterpret_cast<std::byte*>(elem);
            elem->flags = item.flags;
            elem->ksize = item.key.size();
            elem->vsize = item.value.size();
        } else {
            auto elem = p->branchPageElement(i);
            elem->pos = buf - reinterpret_cast<std::byte*>(elem);
            elem->ksize = item.key.size();
            elem->pgid = item.pgid;
            assert("write: circular dependency occurred" && elem->pgid != p->id);
        }

        std::memcpy(buf, item.key.data(), item.key.size());
        buf += item.key.size();
        std::memcpy(buf, item.value.data(), item.value.size());
        buf += item.value.size();
    }
}

std::tuple<bolt::node_ptr, bolt::node_ptr> node::splitTwo(int pageSize) {
    if (inodes.size() <= bolt::minKeysPerPage * 2
        || sizeLessThan(pageSize)) {
        return std::make_tuple(shared_from_this(), nullptr);
    }

    auto fillPercent = bucket->FillPercent;
    if (fillPercent < bolt::minFillPercent) {
        fillPercent = bolt::minFillPercent;
    } else if (fillPercent > bolt::maxFillPercent) {
        fillPercent = maxFillPercent;
    }
    int threshold = (int)(pageSize * fillPercent);
    int splitIdx;
    std::tie(splitIdx, std::ignore) = splitIndex(threshold);
    if (parent == nullptr) {
        auto n = new node(bucket, { shared_from_this()});
        parent = n->shared_from_this();
    }
    auto next = std::make_shared<node>(bucket, isLeaf, parent);
    parent->children.push_back(next);

    std::copy(inodes.begin() + splitIdx, inodes.end(), std::back_inserter(next->inodes));
    inodes.erase(inodes.begin() + splitIdx, inodes.end());

    bucket->tx->stats.Split++;
    return std::make_tuple(shared_from_this(), next);
}

std::tuple<int, int> node::splitIndex(int threshold) {
    int index;
    int sz = bolt::pageHeaderSize;
    for (size_t i = 0; i < inodes.size() - bolt::minKeysPerPage; i++) {
        index = i;
        bolt::inode &inode = inodes.at(i);
        int elsize = pageElementSize() + inode.key.size() + inode.value.size();

        if (i >= bolt::minKeysPerPage && sz + elsize > threshold) {
            break;
        }
        sz += elsize;

    }
    return std::make_tuple(index, sz);
}

bolt::ErrorCode node::spill() {
    auto tx = bucket->tx;
    if (spilled) {
        return bolt::ErrorCode::Success;
    }

    // Spill child nodes first. Child nodes can meterialize sibling nodes in
    // the case of split-merge so we cannot use a range loop. We have to check
    // the children size on every loop iteration
    std::sort(children.begin(), children.end(),
              [](bolt::node *a, bolt::node *b) -> bool {
                return std::lexicographical_compare(
                    a->key.begin(), a->key.end(), b->key.begin(), b->key.end());
              });
    for (int i = 0; i < children.size(); i++) {
        auto err = children[i]->spill();
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
    }

    // We no longger need the child list because it's only used for spill tracking.
    children.clear();

    // Split nodes into appropriate sizes. The first node will always be n.
    auto nodes = split(tx->db->pageSize);
    for (auto it : nodes) {
        // Add node's page to the freelist if it's not new.
        if (it->pgid > 0) {
            tx->db->freelist->free(tx->meta.txid, tx->page(it->pgid));
            it->pgid = 0;
        }

        // Allocate contiguous space for the node.
        auto [p, err] = tx->allocate((it->size() / tx->db->pageSize) + 1);
        if (err != bolt::ErrorCode::Success) {
            return err;
        }

        // Write the node.
        if (p->id >= tx->meta.pgid) {
            assert("pgid above high water mark" && false);
        }
        it->pgid = p->id;
        it->write(p);
        it->spilled = true;

        // Insert into parent inodes.
        if (it->parent != nullptr) {
            auto k = it->key;
            if (k.empty()) {
                k = it->inodes[0].key;
            }
            it->parent->put(key, it->inodes[0].key, bolt::bytes(), it->pgid, 0);
            it->key = it->inodes[0].key;
            assert("spill: zero-length node key" && it->key.size() > 0);
        }
        // Update the statistics.
        tx->stats.Spill++;
    }

    // If the root node split and created a new root then we need to spill that
    // as well. We'll clear out the children to make sure it doesn't try to
    // respill
    if (parent != nullptr && parent->pgid == 0) {
        children.clear();
        return parent->spill();
    }
    return bolt::ErrorCode::Success;
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
void node::rebalance() {
    if (!unbalanced) {
        return;
    }
    unbalanced = false;

    // Update statistics.
    bucket->tx->stats.Rebalance++;

    // Ignore if node is above threshold (25%) and has enough keys.
    int threshold = bucket->tx->db->pageSize / 4;
    if (size() > threshold && inodes.size() > (size_t)minKeys()) {
        return;
    }

    // Root node has special handling.
    if (parent == nullptr) {
        // If root node is a branch and only has one node then collapse it.
        if (isLeaf && inodes.size() == 1) {
            // Move root's child up.
            bolt::node *child = bucket->node(inodes.front().pgid, this);
            isLeaf = child->isLeaf;
            inodes = child->inodes;
            children = child->children;

            // Reparent all child nodes being moved.
            for (auto it : inodes) {
                auto item = bucket->nodes.find(it.pgid);
                if (item != bucket->nodes.end()) {
                    item->second->parent = this;
                }
            }

            // Remove old child.
            child->parent = nullptr;
            auto it = bucket->nodes.find(child->pgid);
            if (it != bucket->nodes.end()) {
                bucket->nodes.erase(it);
            }
            child->free();
        }
        return;
    }

    // If node has no keys then just remove it.
    if (numChildren() == 0) {
        parent->del(key);
        parent->removeChild(this);
        auto it = bucket->nodes.find(pgid);
        if (it != bucket->nodes.end()) {
            bucket->nodes.erase(it);
        }
        this->free();
        parent->rebalance();
        return;
    }

    assert("parent must have at least 2 children" && parent->numChildren() > 1);

    // Destination node is right sibling if idx == 0, otherwise left sibling.
    bolt::node *target;
    bool useNextSibling = parent->childIndex(this) == 0;
    if (useNextSibling) {
        target = nextSibling();
    } else {
        target = prevSibling();
    }

    // If both this node and the target node are too small then merge them.
    if (useNextSibling) {
        // Reparent all child nodes being moved.
        for (auto item : target->inodes) {
            auto it = bucket->nodes.find(item.pgid);
            if (it != bucket->nodes.end()) {
                bolt::node *child = it->second;
                child->parent->removeChild(child);
                child->parent = this;
                child->parent->children.push_back(child);
            }
        }

        // Copy over inodes from target and remove target.
        std::copy(target->inodes.begin(), target->inodes.end(),
                  std::back_inserter(inodes));
        parent->del(target->key);
        parent->removeChild(target);
        auto it = bucket->nodes.find(target->pgid);
        if (it != bucket->nodes.end()) {
            bucket->nodes.erase(it);
        }
        target->free();
    } else {
        // Reparent all child nodes being moved.
        for (auto item : inodes) {
            auto it = bucket->nodes.find(item.pgid);
            if (it != bucket->nodes.end()) {
                bolt::node *child = it->second;
                child->parent->removeChild(child);
                child->parent = target;
                child->parent->children.push_back(child);
            }
        }

        // Copy over inodes to target and remove node.
        std::copy(inodes.begin(), inodes.end(),
                  std::back_inserter(target->inodes));
        parent->del(key);
        parent->removeChild(this);
        auto it = bucket->nodes.find(pgid);
        if (it != bucket->nodes.end()) {
            bucket->nodes.erase(it);
        }
        free();
    }

    // Either this node or the target node was deleted from the parent so
    // rebalance it.
    parent->rebalance();
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
void node::removeChild(bolt::node *target) {
    std::ignore =
        std::remove_if(children.begin(), children.end(),
                       [&](bolt::node *item) { return item == target; });
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
    }
    for (auto &it : inodes) {
        std::vector<std::byte> key, value;
        key.assign(it.key.begin(), it.key.end());
        value.assign(it.value.begin(), it.value.end());

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

    bucket->tx->stats.NodeDeref++;
}

void node::free() {
    if (pgid != 0) {
        bucket->tx->db->freelist->free(bucket->tx->meta.txid, bucket->tx->page(pgid));
        pgid = 0;
    }
}

}
