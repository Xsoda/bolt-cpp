#include "node.hpp"
#include "tx.hpp"
#include <cassert>
#include <algorithm>
#include <cstring>

namespace bolt {

node::node(bolt::Bucket *bucket, std::initianlize_list<bolt::node*> children) {
    this->bucket = bucket;
    this->children = children;
}

node::node(bolt::Bucket *bucket, bool isLeaf, bolt::node *parent) {
    this->bucket = bucket;
    this->parent = parent;
    this->isLeaf = isLeaf;
}


bolt::node *node::root() const {
    if (parent == NULL) {
        return this;
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

bolt::node *node::childAt(int index) const {
    if (isLeaf) {
        assert("invalid chatAt() on a leaf node" && 0);
    }
    return bucket->node(inodes[index].pgid, this);
}

int node::childIndex(const bolt::node *child) const {
    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inode &n) -> bool {
        return std::equal(std::begin(child->key), std::end(child->key),
                          std::begin(n.key), std::end(n.key));
    });
    return std::distance(inodes.begin(), it);
}

int node::numChildren() const {
    return inodes.size();
}

bolt::node *node::nextSibling() const {
    if (parent == nullptr) {
        return nullptr;
    }
    int index = parent->childIndex(this);
    if (index >= parent->numChildren() - 1) {
        return nullptr;
    }
    return parent->childAt(index + 1);
}

bolt::node *node::prevSibling() const {
    if (parent == nullptr) {
        return nullptr;
    }
    int index = parent->childIndex(this);
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

    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inodes &item) -> bool {
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
    inode.set_keyvalue(newKey, value);
    inode.pgid = pgid;
    assert("put: zero-length inode key" && inode.key.size() > 0);
}

void node::del(bolt::bytes key) {
    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inodes &item) -> bool {
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

std::tuple<bolt::node*, bolt::node*> node::splitTwo(int pageSize) {
    if (inodes.size() <= bolt::minKeysPerPage * 2
        || sizeLessThan(pageSize)) {
        return std::make_tuple(this, nullptr);
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
        parent = new node(bucket, { this });
    }
    auto next = new node(bucket, isLeaf, parent);
    parent->children.push_back(next);

    std::copy(inodes.begin() + splitIdx, inodes.end(), std::back_inserter(next->inodes));
    inodes.erase(inodes.begin() + splitIdx, inodes.end());

    bucket->tx->stats.Split++;
    return std::make_tuple(this, next);
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

int node::spill() {
    if (spilled) {
        return 0;
    }
    std::sort(children.begin(), children.end(), [](bolt::node *a, bolt::node *b) -> bool {
        return std::lexicographical_compare(a->key.begin(), a->key.end(),
                                     b->key.begin(), b->key.end());
    });
    for (auto it : children) {
        int err = it->spill();
        if (err) {
            return err;
        }
    }

    children.clear();
    // TODO
}

void node::rebalance() {
    if (!unbalanced) {
        return;
    }
    unbalanced = false;
    bucket->tx->stats.Rebalance++;

    int threshold = bucket->tx->db->pageSize / 4;
    if (size() > threshold && inodes.size() > (size_t)minKeys()) {
        return;
    }

    if (parent == nullptr) {
        if (isLeaf && inodes.size() == 1) {
            bolt::node *child = bucket->node(inodes.front().pgid, this);
            isLeaf = child->isLeaf;
            inodes = child->inodes;
            children = child->children;

            for (auto it : inodes) {
                auto item = bucket->nodes.find(it.pgid);
                if (item != bucket->nodes.end()) {
                    item->second->parent = this;
                }
            }

            child->parent = nullptr;
            auto it = bucket->nodes.find(child->pgid);
            if (it != bucket->nodes.end()) {
                bucket->nodes.erase(it);
            }
            child->free();
        }
        return;
    }

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

    bolt::node *target;
    bool useNextSibling = parent->childIndex(this) == 0;
    if (useNextSibling) {
        target = nextSibling();
    } else {
        target = prevSibling();
    }

    if (useNextSibling) {
        for (auto item : target->inodes) {
            auto it = bucket->nodes(item.pgid);
            if (it != bucket->nodes.end()) {
                bolt::node *child = it->second;
                child->parent->removeChild(child);
                child->parent = this;
                child->parent->children.push_back(child);
            }
        }

        std::copy(target->inodes.begin(), target->inodes.end(), std::back_inserter(inodes));
        parent->del(target->key);
        parent->removeChild(target);
        auto it = bucket->nodes.find(target->pgid);
        if (it != bucket->nodes.end()) {
            bucket->nodes.erase(it);
        }
        target->free();
    } else {
        for (auto item : inodes) {
            auto it = bucket->nodes.find(item.pgid);
            if (it != bucket->nodes.end()) {
                bolt::node *child = it->second;
                child->parent->removeChild(child);
                child->parent = target;
                child->parent->children.push_back(child);
            }
        }

        std::copy(inodes.begin(), inodes.end(), std::back_inserter(target->inodes));
        parent->del(key);
        parent->removeChild(this);
        auto it = bucket->nodes.find(pgid);
        if (it != bucket->nodes.end()) {
            bucket->nodes.erase(it);
        }
        free();
    }
    parent->rebalance();
}

void node::removeChild(bolt::node *target) {
    std::remove_if(children.begin(), children.end(), [&](bolt::node *item) {
        return item == target;
    });
}

void node::dereference() {
    if (key.size() > 0) {
        memory.clear();
        memory.reserve(key.size());
        std::copy(key.begin(), key.end(), std::back_inserter(memory));
        key = bolt::bytes(memory.begin(), memory.end());
    }
    for (auto &it : inodes) {
        it.set_keyvalue(it.key, it.value);
    }

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

void inode::set_keyvalue(bolt::bytes k, bolt::bytes v) {
    memory.clear();
    memory.reserve(k.size() + v.size());
    std::copy(k.begin(), k.end(), std::back_inserter(memory));
    key = bolt::bytes(memory.begin(), memory.end());

    std::copy(v.begin(), v.end(), std::back_inserter(memory));
    value = bolt::bytes(memory.begin() + key.size(), memory.end());
}

}
