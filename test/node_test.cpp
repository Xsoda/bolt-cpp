#include "bolt/error.hpp"
#include "impl/bucket.hpp"
#include "impl/db.hpp"
#include "impl/meta.hpp"
#include "impl/node.hpp"
#include "impl/tx.hpp"
#include "impl/utils.hpp"
#include "test.hpp"
#include "util.hpp"
#include <memory>

TestResult TestNode_put() {
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    auto bar = bolt::to_bytes("bar");
    auto baz = bolt::to_bytes("baz");
    auto foo = bolt::to_bytes("foo");
    auto v0 = bolt::to_bytes("0");
    auto v1 = bolt::to_bytes("1");
    auto v2 = bolt::to_bytes("2");
    auto v3 = bolt::to_bytes("3");
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
    if (!Equal(k, bar) || !Equal(v, v1)) {
        return TestResult(false, "expected inodes[0] is <bar, 1>");
    }
    k = n->inodes[1].key;
    v = n->inodes[1].value;
    if (!Equal(k, baz) || !Equal(v, v2)) {
        return TestResult(false, "expected inodes[1] is <baz, 2>");
    }
    k = n->inodes[2].key;
    v = n->inodes[2].value;
    if (!Equal(k, foo) || !Equal(v, v3)) {
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
    bolt::impl::page *page = reinterpret_cast<bolt::impl::page *>(buf.data());
    page->flags = bolt::impl::leafPageFlag;
    page->count = 2;

    // Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
    bolt::impl::leafPageElement *nodes =
        reinterpret_cast<bolt::impl::leafPageElement *>(&page->ptr);
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

    std::span<std::byte> data = std::span(reinterpret_cast<std::byte *>(&nodes[2]), 4096);
    auto v1 = bolt::to_bytes("barfooz");
    auto v2 = bolt::to_bytes("helloworldbye");
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
    auto n = std::make_shared<bolt::impl::node>(bucket, true, nullptr);
    auto susy = bolt::to_bytes("susy");
    auto que = bolt::to_bytes("que");
    auto ricki = bolt::to_bytes("ricki");
    auto lake = bolt::to_bytes("lake");
    auto john = bolt::to_bytes("john");
    auto johnson = bolt::to_bytes("johnson");
    n->put(susy, susy, que, 0, 0);
    n->put(ricki, ricki, lake, 0, 0);
    n->put(john, john, johnson, 0, 0);

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
    if (!Equal(k, john) || !Equal(v, johnson)) {
        return TestResult(false, "expected inodes[0] is <john, johnson>");
    }

    k = n2->inodes[1].key;
    v = n2->inodes[1].value;
    if (!Equal(k, ricki) || !Equal(v, lake)) {
        return TestResult(false, "expected inodes[1] is <ricki, lake>");
    }

    k = n2->inodes[2].key;
    v = n2->inodes[2].value;
    if (!Equal(k, susy) || !Equal(v, que)) {
        return TestResult(false, "expected inodes[2] is <susy, que>");
    }
    return true;
}

TestResult TestNode_split() {
    std::vector<bolt::impl::node_ptr> hold;
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    auto k1 = bolt::to_bytes("00000001");
    auto k2 = bolt::to_bytes("00000002");
    auto k3 = bolt::to_bytes("00000003");
    auto k4 = bolt::to_bytes("00000004");
    auto k5 = bolt::to_bytes("00000005");
    auto v = bolt::to_bytes("0123456701234567");
    n->put(k1, k1, v, 0, 0);
    n->put(k2, k2, v, 0, 0);
    n->put(k3, k3, v, 0, 0);
    n->put(k4, k4, v, 0, 0);
    n->put(k5, k5, v, 0, 0);

    auto splits = n->split(100, hold);
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
    std::vector<bolt::impl::node_ptr> hold;
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    auto k1 = bolt::to_bytes("00000001");
    auto k2 = bolt::to_bytes("00000002");
    auto v = bolt::to_bytes("0123456701234567");
    n->put(k1, k1, v, 0, 0);
    n->put(k2, k2, v, 0, 0);
    auto split = n->split(20, hold);
    if (!n->parent.expired()) {
        return TestResult(false, "expected nullptr parent");
    }
    return true;
}

TestResult TestNode_split_SinglePage() {
    std::vector<bolt::impl::node_ptr> hold;
    bolt::impl::meta meta(1);
    bolt::impl::DBPtr db = std::make_shared<bolt::impl::DB>();
    bolt::impl::TxPtr tx = std::make_shared<bolt::impl::Tx>(db, meta);
    bolt::impl::BucketPtr bucket = std::make_shared<bolt::impl::Bucket>(tx);
    auto n = std::make_shared<bolt::impl::node>(bucket);
    auto k1 = bolt::to_bytes("00000001");
    auto k2 = bolt::to_bytes("00000002");
    auto k3 = bolt::to_bytes("00000003");
    auto k4 = bolt::to_bytes("00000004");
    auto k5 = bolt::to_bytes("00000005");
    auto v = bolt::to_bytes("0123456701234567");
    n->put(k1, k1, v, 0, 0);
    n->put(k2, k2, v, 0, 0);
    n->put(k3, k3, v, 0, 0);
    n->put(k4, k4, v, 0, 0);
    n->put(k5, k5, v, 0, 0);

    auto splits = n->split(4096, hold);
    if (!n->parent.expired()) {
        return TestResult(false, "expected nullptr parent");
    }
    return true;
}
