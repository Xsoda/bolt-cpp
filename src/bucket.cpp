#include "bucket.hpp"
#include "tx.hpp"
#include "node.hpp"

namespace bolt {

Bucket::Bucket(bolt::Tx *tx): tx(tx) {
    FillPercent = bolt::DefaultFillPercent;
}

bolt::Tx *Bucket::Tx() const {
    return tx;
}

bolt::pgid Bucket::Root() const {
    return bucket->root;
}

bool Bucket::Writable() const {
    return tx->writable;
}
bolt::node *Bucket::node(bolt::pgid pgid, bolt::node *parent) {
    auto it = nodes.find(pgid);
    if (it != nodes.end()) {
        return it->second;
    }
    auto n = new bolt::node(this, false, parent);
    if (parent == nullptr) {
        rootNode = n;
    } else {
        parent->children.push_back(n);
    }

    auto p = page;
    if (p == nullptr) {
        p = tx->page(pgid);
    }
    n->read(p);
    nodes[pgid] = n;
    tx->stats.NodeCount++;
    return n;
}

}
