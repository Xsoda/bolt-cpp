#include "bucket.hpp"
#include "db.hpp"
#include "tx.hpp"
#include "node.hpp"
#include "meta.hpp"
#include "error.hpp"
#include "test.hpp"
#include "utils.hpp"
#include <memory>

template <typename A, typename B>
bool Compare(A a, B b) {
    return std::is_eq(std::lexicographical_compare_three_way(a.begin(), a.end(),
                                        b.begin(), b.end()));
}

std::span<std::byte> to_bytes(std::string &str) {
    return std::span<std::byte>(reinterpret_cast<std::byte*>(str.data()), str.size());
}

TestResult TestNode_put() {
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
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
    n->put(foo, foo, v3, 0, bolt::impl::leafPageFlag);
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
    if (n->inodes[2].flags != std::uint32_t(bolt::impl::leafPageFlag)) {
        return TestResult(false, "not a leaf page");
    }
    return true;
}

TestResult TestNode_read_LeafPage() {
    std::vector<std::byte> buf;
    buf.assign(4096, std::byte(0));
    bolt::impl::page *page = reinterpret_cast<bolt::impl::page*>(buf.data());
    page->flags = bolt::impl::leafPageFlag;
    page->count = 2;

    // Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
    bolt::impl::leafPageElement *nodes = reinterpret_cast<bolt::impl::leafPageElement*>(&page->ptr);
    bolt::impl::leafPageElement elem;
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

    auto n = std::make_shared<bolt::impl::node>();
    n->read(page);
    if (!n->isLeaf) {
        return TestResult(false, "expected leaf");
    }
    if (n->inodes.size() != 2) {
        return TestResult(false, "expected inodes length is 2");
    }
    return true;
}

TestResult TestNode_write_LeafPage() {
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    std::string susy = "susy";
    std::string que = "que";
    std::string ricki = "ricki";
    std::string lake = "lake";
    std::string john = "john";
    std::string johnson = "johnson";
    std::span<std::byte> s_susy(reinterpret_cast<std::byte*>(susy.data()), susy.size());
    std::span<std::byte> s_que(reinterpret_cast<std::byte*>(que.data()), que.size());
    std::span<std::byte> s_ricki(reinterpret_cast<std::byte*>(ricki.data()), ricki.size());
    std::span<std::byte> s_lake(reinterpret_cast<std::byte*>(lake.data()), lake.size());
    std::span<std::byte> s_john(reinterpret_cast<std::byte*>(john.data()), john.size());
    std::span<std::byte> s_johnson(reinterpret_cast<std::byte*>(johnson.data()), johnson.size());
    n->put(s_susy, s_susy, s_que, 0, 0);
    n->put(s_ricki, s_ricki, s_lake, 0, 0);
    n->put(s_john, s_john, s_johnson, 0, 0);

    std::vector<std::byte> buf;
    buf.assign(4096, std::byte(0));
    bolt::impl::page *p = reinterpret_cast<bolt::impl::page *>(buf.data());
    n->write(p);

    auto n2 = std::make_shared<bolt::impl::node>();
    n2->read(p);
    if (n2->inodes.size() != 3) {
        return TestResult(false, "expected inodes size is 3");
    }
    std::span<std::byte> k, v;
    k = n2->inodes[0].key;
    v = n2->inodes[0].value;
    if (!Compare(k, s_john) || !Compare(v, s_johnson)) {
        return TestResult(false, "expected inodes[0] is <john, johnson>");
    }

    k = n2->inodes[1].key;
    v = n2->inodes[1].value;
    if (!Compare(k, s_ricki) || !Compare(v, s_lake)) {
        return TestResult(false, "expected inodes[1] is <ricki, lake>");
    }

    k = n2->inodes[2].key;
    v = n2->inodes[2].value;
    if (!Compare(k, s_susy) || !Compare(v, s_que)) {
        return TestResult(false, "expected inodes[2] is <susy, que>");
    }
    return true;
}

TestResult TestNode_split() {
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    std::string k1 = "00000001";
    std::string k2 = "00000002";
    std::string k3 = "00000003";
    std::string k4 = "00000004";
    std::string k5 = "00000005";
    std::string v = "0123456701234567";
    auto s_k1 = to_bytes(k1);
    auto s_k2 = to_bytes(k2);
    auto s_k3 = to_bytes(k3);
    auto s_k4 = to_bytes(k4);
    auto s_k5 = to_bytes(k5);
    auto s_v = to_bytes(v);
    n->put(s_k1, s_k1, s_v, 0, 0);
    n->put(s_k2, s_k2, s_v, 0, 0);
    n->put(s_k3, s_k3, s_v, 0, 0);
    n->put(s_k4, s_k4, s_v, 0, 0);
    n->put(s_k5, s_k5, s_v, 0, 0);

    auto splits = n->split(100);
    auto parent = n->parent.lock();
    if (parent->children.size() != 2) {
        return TestResult(false, "expected parent->children size is 2");
    }
    if (parent->children[0]->inodes.size() != 2) {
        return TestResult(false, "expected parent->children[0] inodes is 2");
    }
    if (parent->children[1]->inodes.size() != 3) {
        return TestResult(false, "expected parent->children[1] inodes is 3");
    }
    return true;
}

TestResult TestNode_split_MinKeys() {
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    std::string k1 = "00000001";
    std::string k2 = "00000002";
    std::string v = "0123456701234567";
    auto s_k1 = to_bytes(k1);
    auto s_k2 = to_bytes(k2);
    auto s_v = to_bytes(v);
    n->put(s_k1, s_k1, s_v, 0, 0);
    n->put(s_k2, s_k2, s_v, 0, 0);
    auto split = n->split(20);
    if (!n->parent.expired()) {
        return TestResult(false, "expected nullptr parent");
    }
    return true;
}

TestResult TestNode_split_SinglePage() {
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    std::string k1 = "00000001";
    std::string k2 = "00000002";
    std::string k3 = "00000003";
    std::string k4 = "00000004";
    std::string k5 = "00000005";
    std::string v = "0123456701234567";
    auto s_k1 = to_bytes(k1);
    auto s_k2 = to_bytes(k2);
    auto s_k3 = to_bytes(k3);
    auto s_k4 = to_bytes(k4);
    auto s_k5 = to_bytes(k5);
    auto s_v = to_bytes(v);
    n->put(s_k1, s_k1, s_v, 0, 0);
    n->put(s_k2, s_k2, s_v, 0, 0);
    n->put(s_k3, s_k3, s_v, 0, 0);
    n->put(s_k4, s_k4, s_v, 0, 0);
    n->put(s_k5, s_k5, s_v, 0, 0);

    auto splits = n->split(4096);
    if (!n->parent.expired()) {
        return TestResult(false, "expected nullptr parent");
    }
    return true;
}
