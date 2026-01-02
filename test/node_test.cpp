#include "bucket.hpp"
#include "db.hpp"
#include "tx.hpp"
#include "node.hpp"
#include "meta.hpp"
#include "test.hpp"
#include <memory>

template <typename A, typename B>
bool Compare(A a, B b) {
    return std::is_eq(std::lexicographical_compare_three_way(a.begin(), a.end(),
                                        b.begin(), b.end()));
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

TestResult TestNode_read_LeafPage() {
    std::vector<std::byte> buf;
    buf.assign(4096, std::byte(0));
    bolt::page *page = reinterpret_cast<bolt::page*>(buf.data());
    page->flags = bolt::leafPageFlag;
    page->count = 2;

    // Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
    bolt::leafPageElement *nodes = reinterpret_cast<bolt::leafPageElement*>(&page->ptr);
    bolt::leafPageElement elem;
    elem.flags = 0;
    elem.pos = 32;
    elem.ksize = 3;
    elem.vsize = 4;
    nodes[0] = elem;

    elem.pos = 23;
    elem.ksize = 10;
    elem.vsize = 3;
    nodes[1] = elem;

    std::span<std::byte> data = std::span(reinterpret_cast<std::byte*>(&nodes[2]), 4096);
    std::vector<std::byte> v1 = {std::byte('b'), std::byte('a'), std::byte('r'),
                                 std::byte('f'), std::byte('o'), std::byte('o'),
                                 std::byte('z')};
    std::vector<std::byte> v2 = {std::byte('h'), std::byte('e'), std::byte('l'),
                                 std::byte('l'), std::byte('o'), std::byte('w'),
                                 std::byte('o'), std::byte('r'), std::byte('l'),
                                 std::byte('d'), std::byte('b'), std::byte('y'),
                                 std::byte('e')};
    std::copy(v1.begin(), v1.end(), data.begin());
    std::copy(v2.begin(), v2.end(), data.begin() + 7);

    auto n = std::make_shared<bolt::node>();
    n->read(page);
    if (!n->isLeaf) {
        return TestResult(false, "expected leaf");
    }
    std::cout << "inodes length " << n->inodes.size() << std::endl;
    if (!n->inodes.size() != 2) {
        return TestResult(false, "expected inodes length is 2");
    }
    return true;
}

TestResult TestNode_write_LeafPage() { return true; }

TestResult TestNode_split() { return true; }

TestResult TestNode_split_MinKeys() { return true; }

TestResult TestNode_split_SinglePage() { return true; }
