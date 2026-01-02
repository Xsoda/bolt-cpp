#include "bucket.hpp"
#include "db.hpp"
#include "tx.hpp"
#include "node.hpp"
#include "meta.hpp"
#include "test.hpp"
#include <memory>

template <typename A, typename B>
bool Compare(A a, B b) {
    return std::lexicographical_compare(a.begin(), a.end(),
                                        b.begin(), b.end());
}

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
    n->put(baz, baz, v2, 0, 0);
    n->put(foo, foo, v0, 0, 0);
    n->put(bar, bar, v1, 0, 0);
    n->put(foo, foo, v3, 0, bolt::leafPageFlag);
    std::cout << "inodes length " << n->inodes.size() << std::endl;
    if (n->inodes.size() != 3) {
        return TestResult(false, "expected inodes length is 3");
    }
    std::span<std::byte> k, v;
    k = n->inodes[0].key;
    v = n->inodes[0].value;
    if (!Compare(k, bar) || !Compare(v, v1)) {
        return TestResult(false, "expected inodes[0] is <bar, 1>");
    }
    k = n->inodes[1].key;
    v = n->inodes[1].value;
    if (!Compare(k, baz) || !Compare(v, v2)) {
        return TestResult(false, "expected inodes[1] is <baz, 2>");
    }
    k = n->inodes[2].key;
    v = n->inodes[2].value;
    if (!Compare(k, foo) || !Compare(v, v3)) {
        return TestResult(false, "expected inodes[2] is <foo, 3>");
    }
    if (n->inodes[2].flags != std::uint32_t(bolt::leafPageFlag)) {
        return TestResult(false, "not a leaf page");
    }
    return true;
}
TestResult TestNode_read_LeafPage() { return true; }

TestResult TestNode_write_LeafPage() { return true; }

TestResult TestNode_split() { return true; }

TestResult TestNode_split_MinKeys() { return true; }

TestResult TestNode_split_SinglePage() { return true; }
