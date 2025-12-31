#include "bucket.hpp"
#include "db.hpp"
#include "tx.hpp"
#include "node.hpp"
#include "meta.hpp"
#include "test.hpp"

bolt::node_ptr create_node() {
    bolt::meta meta(1);
    bolt::DBPtr db = std::make_shared<bolt::DB>();
    bolt::TxPtr tx = std::make_shared<bolt::Tx>(db, meta);
    bolt::BucketPtr bucket = std::make_shared<bolt::Bucket>(tx);
    return std::make_shared<bolt::node>(bucket);
}

TestResult TestNode_put() { return true; }

TestResult TestNode_read_LeafPage() { return true; }

TestResult TestNode_write_LeafPage() { return true; }

TestResult TestNode_split() { return true; }

TestResult TestNode_split_MinKeys() { return true; }

TestResult TestNode_split_SinglePage() { return true; }
