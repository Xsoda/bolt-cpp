#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/page.hpp"
#include "impl/cursor.hpp"
#include "random.hpp"
#include "test.hpp"
#include "util.hpp"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

extern std::span<std::byte> to_bytes(std::string &str);
extern bolt::impl::DBPtr MustOpenDB();
extern void MustCloseDB(bolt::impl::DBPtr &&db);

TestResult TestCursor_Bucket() {
    auto db = MustOpenDB();
    auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "widgets";
        auto [b, err] = tx->CreateBucket(to_bytes(name));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        auto c = b->Cursor();
        auto cb = c->Bucket();
        if (b != cb) {
            return bolt::ErrorCode::ErrorBucketNotFound;
        }
        return bolt::ErrorCode::Success;
    });
    if (err != bolt::ErrorCode::Success) {
        return TestResult("cursor bucket mismatch");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_Seek() {
    auto db = MustOpenDB();
    fmt::println("database path: {}", db->Path());
    auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string baz = "baz";
        std::string bkt = "bkt";
        std::vector<std::string> val = {"0001", "0002", "0003"};
        auto [b, err] = tx->CreateBucket(to_bytes(name));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        err = b->Put(to_bytes(foo), to_bytes(val[0]));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        err = b->Put(to_bytes(bar), to_bytes(val[1]));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        err = b->Put(to_bytes(baz), to_bytes(val[2]));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        std::tie(b, err) = b->CreateBucket(to_bytes(bkt));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        return bolt::ErrorCode::Success;
    });
    if (err != bolt::ErrorCode::Success) {
        return TestResult("update database fail");
    }
    // err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
    //     std::string name = "widgets";
    //     std::string foo = "foo";
    //     std::string bar = "bar";
    //     std::string bas = "bas";
    //     std::string baz = "baz";
    //     std::string bkt = "bkt";
    //     std::vector<std::string> val = {"0001", "0002", "0003"};
    //     auto c = tx->Bucket(to_bytes(name))->Cursor();
    //     auto [k, v] = c->Seek(to_bytes(bar));
    //     if (!Compare(k, to_bytes(bar)) || !Compare(v, to_bytes(val[1]))) {
    //         return bolt::ErrorCode::ErrorUnexpected;
    //     }
    //     std::tie(k, v) = c->Seek(to_bytes(bas));
    //     if (!Compare(k, to_bytes(baz)) || !Compare(v, to_bytes(val[2]))) {
    //         return bolt::ErrorCode::ErrorUnexpected;
    //     }
    //     return bolt::ErrorCode::Success;
    // });
    // if (err != bolt::ErrorCode::Success) {
    //     return TestResult("view database fail");
    // }
    MustCloseDB(std::move(db));
    return true;
}
