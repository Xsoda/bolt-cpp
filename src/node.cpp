#include "node.hpp"
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

}

void node::del(bolt::bytes key) {
    auto it = std::find_if(inodes.begin(), inodes.end(), [&](bolt::inodes &item) -> bool {
        return std::equal(key.begin(), key.end(),
                          item.key.begin(), item.key.end());
    });
    if (it == inodes.end()
        || !std::equal(item.key.begin(), item.key.end(),
                       key.begin(), key.end())) {
        return
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

    // bucket.tx.stats.Split++;
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


}
