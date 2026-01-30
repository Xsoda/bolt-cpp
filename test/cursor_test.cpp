#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/page.hpp"
#include "impl/cursor.hpp"
#include "random.hpp"
#include "test.hpp"
#include "util.hpp"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <concepts>
#include <bit>

extern std::span<std::byte> to_bytes(std::string &str);
extern std::string to_string(std::span<std::byte> s);
extern bolt::impl::DBPtr MustOpenDB();
extern void MustCloseDB(bolt::impl::DBPtr &&db);

TestResult TestCursor_Bucket() {
    auto db = MustOpenDB();
    auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "widgets";
        auto [b, err] = tx->CreateBucket(to_bytes(name));
        if (err != bolt::Success) {
            return err;
        }
        auto c = b->Cursor();
        auto cb = c->Bucket();
        if (b != cb) {
            return bolt::ErrorBucketNotFound;
        }
        return bolt::Success;
    });
    if (err != bolt::Success) {
        return TestResult(false, "cursor bucket mismatch");
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
        if (err != bolt::Success) {
            return err;
        }
        err = b->Put(to_bytes(foo), to_bytes(val[0]));
        if (err != bolt::Success) {
            return err;
        }
        err = b->Put(to_bytes(bar), to_bytes(val[1]));
        if (err != bolt::Success) {
            return err;
        }
        err = b->Put(to_bytes(baz), to_bytes(val[2]));
        if (err != bolt::Success) {
            return err;
        }
        std::tie(b, err) = b->CreateBucket(to_bytes(bkt));
        if (err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
    if (err != bolt::Success) {
        return TestResult(false, "update database fail");
    }
    err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string bas = "bas";
        std::string baz = "baz";
        std::string bkt = "bkt";
        std::string zzz = "zzz";
        std::vector<std::string> val = {"0001", "0002", "0003"};
        auto c = tx->Bucket(to_bytes(name))->Cursor();
        auto [k, v] = c->Seek(to_bytes(bar));
        if (!Compare(k, to_bytes(bar)) || !Compare(v, to_bytes(val[1]))) {
            fmt::println("unexpected key: {}, value: {}", to_string(k),
                         to_string(v));
            fmt::println("expected key: {}, value: {}", bar, val[1]);
            return bolt::ErrorUnexpected;
        }
        std::tie(k, v) = c->Seek(to_bytes(bas));
        if (!Compare(k, to_bytes(baz)) || !Compare(v, to_bytes(val[2]))) {
            fmt::println("unexpected key: {}, value: {}", to_string(k),
                         to_string(v));
            fmt::println("expected key: {}, value: {}", baz, val[2]);
            return bolt::ErrorUnexpected;
        }
        if (std::tie(k, v) = c->Seek(bolt::bytes());
            !Compare(k, to_bytes(bar))) {
            fmt::println("unexpected key: {}", to_string(k));
            return bolt::ErrorUnexpected;
        } else if (!Compare(v, to_bytes(val[1]))) {
            fmt::println("unexpected value: {}", to_string(v));
            return bolt::ErrorUnexpected;
        }
        if (std::tie(k, v) = c->Seek(to_bytes(zzz)); !k.empty()) {
            fmt::println("unexpected key: {}", to_string(k));
            return bolt::ErrorUnexpected;
        } else if (!v.empty()) {
            fmt::println("unexpected value: {}", to_string(v));
            return bolt::ErrorUnexpected;
        }
        if (std::tie(k, v) = c->Seek(to_bytes(bkt));
            !Compare(k, to_bytes(bkt))) {
            fmt::println("unexpected key: {}", to_string(k));
            return bolt::ErrorUnexpected;
        } else if (!v.empty()) {
            fmt::println("unexpected value: {}", to_string(v));
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
    if (err != bolt::Success) {
        return TestResult(false, "view database fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

template <std::integral T> constexpr T byteswap(T value) noexcept {
    union {
        T val;
        char ptr[sizeof(T)];
    } s;
    s.val = value;
    for (int i = 0; i < sizeof(T) / 2; i++) {
        auto tmp = s.ptr[i];
        s.ptr[i] = s.ptr[sizeof(T) - i - 1];
        s.ptr[sizeof(T) - i - 1] = tmp;
    }
    return s.val;
}
TestResult TestCursor_Delete() {
    auto db = MustOpenDB();
    const int count = 50000;
    std::vector<std::string> keys;
    keys.reserve(count);
    if (auto err = db->Update([&keys, count](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string sub = "sub";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        for (int i = 0; i < count; i++) {
            std::uint64_t k = i;
            std::vector<std::byte> value;
            value.assign(100, std::byte(0));
            if constexpr (std::endian::native == std::endian::little) {
                k = byteswap(k);
            }
            std::span<std::byte> key = std::span<std::byte>{
                reinterpret_cast<std::byte *>(&k), sizeof(std::uint64_t)};
            std::span<std::byte> val = std::span<std::byte>{
                reinterpret_cast<std::byte *>(value.data()), value.size()};
            if (auto err = b->Put(key, val); err != bolt::Success) {
                return err;
            }
            // auto k = RandomCharset(8);
            // auto v = RandomCharset(100);
            // keys.push_back(k);
            // if (auto err = b->Put(to_bytes(k), to_bytes(v));
            //     err != bolt::Success) {
            //     return err;
            // }
        }
        if (std::tie(b, err) = b->CreateBucket(to_bytes(sub));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "1. unexpected error: {}", err);
    }
    fmt::println("------------------------------------------");
    if (auto err =
        db->Update([&keys, count](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            std::string widgets = "widgets";
            std::string sub = "sub";
            auto c = tx->Bucket(to_bytes(widgets))->Cursor();
            std::uint64_t m = count / 2;
            // std::sort(keys.begin(), keys.end());
            auto b = c->Bucket();
            if constexpr (std::endian::native == std::endian::little) {
              m = byteswap(m);
            }
            std::span<std::byte> bound = std::span<std::byte>{
                reinterpret_cast<std::byte *>(&m),
                sizeof(std::uint64_t)};
            // std::span<std::byte> bound = std::span<std::byte>{
            //     reinterpret_cast<std::byte *>(keys[m].data()),
            //     keys[m].size()};
            auto [k, v] = c->First();
            while (std::is_lt(std::lexicographical_compare_three_way(
                                                                     k.begin(), k.end(), bound.begin(), bound.end()))) {
                if (auto err = c->Delete(); err != bolt::Success) {
                    return err;
                }
                std::tie(k, v) = c->Next();
            }

            c->Seek(to_bytes(sub));
            if (auto err = c->Delete();
                err != bolt::ErrorIncompatiableValue) {
                return err;
            }
            b->dump();
            return bolt::Success;
        });
        err != bolt::Success) {
        return TestResult(false, "2. unexpected error: {}", err);
    }
    if (auto err = db->View([count](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "3. unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}
