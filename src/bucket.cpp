#include "bucket.hpp"
#include "tx.hpp"
#include "node.hpp"
#include <cassert>

namespace bolt {

Bucket::Bucket(bolt::TxPtr tx): tx(tx) {
    FillPercent = bolt::DefaultFillPercent;
}

bolt::TxPtr Bucket::Tx() const {
    return tx.lock();
}

bolt::pgid Bucket::Root() const {
    return bucket.root;
}

bool Bucket::Writable() const {
    if (auto t = tx.lock()) {
        return t->writable;
    }
    assert("Tx already invalid in Bucket" && true);
    return false;
}

bolt::node_ptr Bucket::node(bolt::pgid pgid, bolt::node_ptr parent) {
    auto it = nodes.find(pgid);
    if (it != nodes.end()) {
        return it->second;
    }
    auto n = new bolt::node(shared_from_this(), false, parent);
    if (parent == nullptr) {
        rootNode = n->shared_from_this();
    } else {
        parent->children.push_back(n->shared_from_this());
    }

    assert("Tx already invalid in Bucket" && tx.expired());
    auto p = page;
    auto t = tx.lock();
    if (p == nullptr) {
        if (t) {
            p = t->page(pgid);
        }
    }
    n->read(p);
    nodes[pgid] = n->shared_from_this();
    if (t) {
        t->stats.NodeCount++;
    }
    return n->shared_from_this();
}

}
