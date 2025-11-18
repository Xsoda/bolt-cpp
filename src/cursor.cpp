#include "cursor.hpp"
#include "node.hpp"
#include "page.hpp"
#include <cassert>

namespace bolt {

bool elemRef::isLeaf() const {
    if (this->node != nullptr) {
        return this->node->isLeaf;
    }
    return (this->page->flags & bolt::leafPageFlag) != 0;
}

int elemRef::count() const {
    if (this->node != nullptr) {
        return this->node->inodes.size();
    }
    return this->page->count;
}

bolt::node *Cursor::node() const {
    size_t len = stack.size();
    assert(len > 0);
    auto &ref = stack.back();
    if (ref.node != nullptr && ref.isLeaf()) {
        return ref.node;
    }
    bolt::node *n = stack.front().node;
    if (n == nullptr) {
        n = bucket->node(stack.front().page->id, nullptr);
    }
    for (size_t i = 0; i < len - 1; i++) {
        auto ref = stack.at(i);
        assert(!n->isLeaf);
        n = n->childAt(ref.index);
    }
    assert(n->isLeaf);
    return n;
}

std::tuple<bolt::pair, std::uint32_t> Cursor::keyValue() {
    auto ref = stack.back();
    if (ref.count() == 0 || ref.index >= ref.count()) {
        auto kv = std::make_pair(bolt::bytes(nullptr, 0), bolt::bytes(nullptr, 0));
        return std::make_tuple<bolt::pair, std::uint32_t>(kv, 0);
    }

    if (ref.node != nullptr) {
        auto inode = ref.node->inodes.at(ref.index);
        auto kv = std::make_pair(inode.key, inode.value);
        return std::make_tuple(kv, inode.flags);
    }

    bolt::leafPageElement *elem = ref.page->leafPageElement(ref.index);
    auto kv = std::make_pair(elem->key(), elem->value());
    return std::make_tuple(kv, elem->flags);
}

}
