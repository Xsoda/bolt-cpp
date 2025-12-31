#include "bucket.hpp"
#include "db.hpp"
#include "tx.hpp"
#include "node.hpp"
#include "meta.hpp"
#include "test.hpp"
#include <memory>

TestResult TestNode_put() {
    bolt::meta meta(1);
    bolt::DBPtr db = std::make_shared<bolt::DB>();
    bolt::TxPtr tx = std::make_shared<bolt::Tx>(db, meta);
    bolt::BucketPtr bucket = std::make_shared<bolt::Bucket>(tx);
    auto n = std::make_shared<bolt::node>(bucket);
    std::vector<std::byte> bar = {std::byte('b'), std::byte('a'), std::byte('r')};
    std::vector<std::byte> baz = {std::byte('b'), std::byte('a'), std::byte('z')};
    std::vector<std::byte> foo = {std::byte('f'), std::byte('o'), std::byte('o')};
    std::vector<std::byte> v0 = {std::byte('0')};
    std::vector<std::byte> v1 = {std::byte('1')};
    std::vector<std::byte> v2 = {std::byte('2')};
    std::vector<std::byte> v3 = {std::byte('3')};
    n->put(bar, bar, v2, 0, 0);
    n->put(foo, foo, v0, 0, 0);
    n->put(bar, bar, v1, 0, 0);
    n->put(foo, foo, v3, 0, bolt::leafPageFlag);
    return true;
}
TestResult TestNode_read_LeafPage() { return true; }

TestResult TestNode_write_LeafPage() { return true; }

TestResult TestNode_split() { return true; }

TestResult TestNode_split_MinKeys() { return true; }

TestResult TestNode_split_SinglePage() { return true; }
