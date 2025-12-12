#include "test.hpp"
#include "common.hpp"
#include "node.hpp"
#include "tx.hpp"
#include <memory>

TestResult TestNodePut() {
    bolt::TxPtr t = std::make_shared<bolt::Tx>();
    bolt::BucketPtr b = std::make_shared<bolt::Bucket>(t);
    bolt::node_ptr n = std::make_shared<bolt::node>(b);

    t->meta.pgid = 1;
    b->tx = t;
    n->bucket = b;


    return TestResult(true);
}
