#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/page.hpp"
#include "impl/cursor.hpp"
#include "impl/bsearch.hpp"
#include "random.hpp"
#include "test.hpp"
#include "util.hpp"
#include "quick_check.hpp"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <concepts>
#include <bit>

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
        if (!Equal(k, to_bytes(bar)) || !Equal(v, to_bytes(val[1]))) {
          fmt::println("unexpected key: {}, value: {}", k, v);
            fmt::println("expected key: {}, value: {}", bar, val[1]);
            return bolt::ErrorUnexpected;
        }
        std::tie(k, v) = c->Seek(to_bytes(bas));
        if (!Equal(k, to_bytes(baz)) || !Equal(v, to_bytes(val[2]))) {
            fmt::println("unexpected key: {}, value: {}", k, v);
            fmt::println("expected key: {}, value: {}", baz, val[2]);
            return bolt::ErrorUnexpected;
        }
        if (std::tie(k, v) = c->Seek(bolt::bytes());
            !Equal(k, to_bytes(bar))) {
            fmt::println("unexpected key: {}", k);
            return bolt::ErrorUnexpected;
        } else if (!Equal(v, to_bytes(val[1]))) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        if (std::tie(k, v) = c->Seek(to_bytes(zzz)); !k.empty()) {
            fmt::println("unexpected key: {}", k);
            return bolt::ErrorUnexpected;
        } else if (!v.empty()) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        if (std::tie(k, v) = c->Seek(to_bytes(bkt));
            !Equal(k, to_bytes(bkt))) {
            fmt::println("unexpected key: {}", k);
            return bolt::ErrorUnexpected;
        } else if (!v.empty()) {
            fmt::println("unexpected value: {}", v);
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
    const int count = 1000;
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

            while (std::is_lt(bolt::impl::compare_three_way(k, bound))) {
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
        std::string widgets = "widgets";
        auto stats = tx->Bucket(to_bytes(widgets))->Stats();
        if (stats.KeyN != count / 2 + 1) {
            fmt::println("unexpected KeyN: %d", stats.KeyN);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "3. unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_Seek_Large() {
    auto db = MustOpenDB();
    int count = 10000;
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::vector<std::byte> value;
        value.assign(100, std::byte(0));
        std::uint64_t k;
        auto key = std::span<std::byte>{reinterpret_cast<std::byte *>(&k),
                                        sizeof(std::uint64_t)};
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        for (auto i = 0; i < count; i += 100) {
            for (auto j = i; j < i + 100; j += 2) {
                k = j;
                if constexpr (std::endian::native == std::endian::little) {
                    k = byteswap(k);
                }
                if (auto err = b->Put(key, to_bytes(value));
                    err != bolt::Success) {
                    return err;
                }
            }
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::uint64_t k, num;
        auto c = tx->Bucket(to_bytes(widgets))->Cursor();
        auto key = std::span<std::byte>{reinterpret_cast<std::byte *>(&k),
                                        sizeof(std::uint64_t)};
        for (auto i = 0; i < count; i++) {
            k = i;
            if constexpr (std::endian::native == std::endian::little) {
                k = byteswap(k);
            }
            auto [seek, val] = c->Seek(key);
            if (i == count - 1) {
                if (!seek.empty()) {
                    return bolt::ErrorUnexpected;
                }
                continue;
            }
            num = reinterpret_cast<const std::uint64_t *>(seek.data())[0];
            if constexpr (std::endian::native == std::endian::little) {
                num = byteswap(num);
            }
            if (i % 2 == 0) {
                if (num != std::uint64_t(i)) {
                    fmt::println("unexpected num: {}", num);
                    return bolt::ErrorUnexpected;
                }
            } else {
                if (num != std::uint64_t(i + 1)) {
                    fmt::println("unexpected num: {} != {}", num, i + 1);
                    return bolt::ErrorUnexpected;
                }
            }
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_EmptyBucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          return err;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        auto c = tx->Bucket(to_bytes(widgets))->Cursor();
        auto [k, v] = c->First();
        if (!k.empty()) {
            fmt::println("unexpected key: {}", k);
            return bolt::ErrorUnexpected;
        } else if (!v.empty()) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true; }

TestResult TestCursor_EmptyBucketReverse() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          return err;
        });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          auto c = tx->Bucket(to_bytes(widgets))->Cursor();
          auto [k, v] = c->Last();
          if (!k.empty()) {
            fmt::println("unexpected key: {}", k);
            return bolt::ErrorUnexpected;
          } else if (!v.empty()) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
          }
          return bolt::Success;
        });
        err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_Iterate_Leaf() {
    std::string widgets = "widgets";
    std::string baz = "baz";
    std::string foo = "foo";
    std::string bar = "bar";
    std::string vempty = "";
    std::string v0 = "\x00";
    std::string v1 = "\x01";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(baz), to_bytes(vempty));
            err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(v0)); err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(bar), to_bytes(v1)); err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    auto [tx, err] = db->Begin(false);
    if (err != bolt::Success) {
        return TestResult(false, "Begin tx fail, {}", err);
    }
    auto c = tx->Bucket(to_bytes(widgets))->Cursor();

    auto [k, v] = c->First();
    if (!Equal(k, to_bytes(bar))) {
        return TestResult(false, "unexpected key: {}", k);
    } else if (!Equal(v, to_bytes(v1))) {
        return TestResult(false, "unexpected value: {}", v);
    }

    std::tie(k, v) = c->Next();
    if (!Equal(k, to_bytes(baz))) {
        return TestResult(false, "unexpected key: {}", k);
    } else if (!Equal(v, to_bytes(vempty))) {
        return TestResult(false, "unexpected value: {}", v);
    }

    std::tie(k, v) = c->Next();
    if (!Equal(k, to_bytes(foo))) {
        return TestResult(false, "unexpected key: {}", k);
    } else if (!Equal(v, to_bytes(v0))) {
        return TestResult(false, "unexpected value: {}", v);
    }

    std::tie(k, v) = c->Next();
    if (!k.empty()) {
        return TestResult(false, "expected nil key: {}", k);
    } else if (!v.empty()) {
        return TestResult(false, "expected nil value: {}", v);
    }

    std::tie(k, v) = c->Next();
    if (!k.empty()) {
        return TestResult(false, "unexpected nil key: {}", k);
    } else if (!v.empty()) {
        return TestResult(false, "unexpected nil value: {}", v);
    }
    if (err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "Rollback fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_LeafRootReverse() {
    std::string widgets = "widgets";
    std::string foo = "foo";
    std::string baz = "baz";
    std::string bar = "bar";
    std::string vempty = "";
    std::string v0 = "\x00";
    std::string v1 = "\x01";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          if (err != bolt::Success) {
            return err;
          }
          if (err = b->Put(to_bytes(baz), to_bytes(vempty));
              err != bolt::Success) {
            return err;
          }
          if (err = b->Put(to_bytes(foo), to_bytes(v0)); err != bolt::Success) {
            return err;
          }
          if (err = b->Put(to_bytes(bar), to_bytes(v1)); err != bolt::Success) {
            return err;
          }
          return bolt::Success;
        });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    auto [tx, err] = db->Begin(false);
    if (err != bolt::Success) {
        return TestResult(false, "Begin tx fail, {}", err);
    }
    auto c = tx->Bucket(to_bytes(widgets))->Cursor();

    if (auto [k, v] = c->Last(); !Equal(k, to_bytes(foo))) {
        return TestResult(false, "unexpected key: {}", k);
    } else if (!Equal(v, to_bytes(v0))) {
        return TestResult(false, "unexpected value: {}", v);
    }

    if (auto [k, v] = c->Prev(); !Equal(k, to_bytes(baz))) {
        return TestResult(false, "unexpected key: {}", k);
    } else if (!Equal(v, to_bytes(vempty))) {
        return TestResult(false, "unexpected value: {}", v);
    }

    if (auto [k, v] = c->Prev(); !Equal(k, to_bytes(bar))) {
        return TestResult(false, "unexpected key: {}", k);
    } else if (!Equal(v, to_bytes(v1))) {
        return TestResult(false, "unexpected value: {}", v);
    }

    if (auto [k, v] = c->Prev(); !k.empty()) {
        return TestResult(false, "expected nil key: {}", k);
    } else if (!v.empty()) {
        return TestResult(false, "expected nil value: {}", v);
    }

    if (auto [k, v] = c->Prev(); !k.empty()) {
        return TestResult(false, "expected nil key: {}", k);
    } else if (!v.empty()) {
        return TestResult(false, "expected nil value: {}", v);
    }

    if (auto err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "rollback error, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_Restart() {
    std::string widgets = "widgets";
    std::string empty = "";
    std::string bar = "bar";
    std::string foo = "foo";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          if (err != bolt::Success) {
              return err;
          }
          if (err = b->Put(to_bytes(bar), to_bytes(empty));
              err != bolt::Success) {
              return err;
          }
          if (err = b->Put(to_bytes(foo), to_bytes(empty));
              err != bolt::Success) {
              return err;
          }
          return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    auto [tx, err] = db->Begin(false);
    if (err != bolt::Success) {
        return TestResult(false, "begin tx fail, {}", err);
    }
    auto c = tx->Bucket(to_bytes(widgets))->Cursor();
    if (auto [k, v] = c->First(); !Equal(k, to_bytes(bar))) {
        return TestResult(false, "unexpected key: {}", k);
    }
    if (auto [k, v] = c->Next(); !Equal(k, to_bytes(foo))) {
        return TestResult(false, "unexpected key: {}", k);
    }

    if (auto [k, v] = c->First(); !Equal(k, to_bytes(bar))) {
        return TestResult(false, "unexpected key: {}", k);
    }
    if (auto [k, v] = c->Next(); !Equal(k, to_bytes(foo))) {
        return TestResult(false, "unexpected key: {}", k);
    }

    if (err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "rollback fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

std::span<const std::byte> u64tob(std::uint64_t &v) {
    if constexpr (std::endian::native == std::endian::little) {
        v = byteswap(v);
    }
    return std::span<const std::byte>(reinterpret_cast<const std::byte*>(&v), sizeof(std::uint64_t));
}

TestResult TestCursor_First_EmptyPages() {
    std::string widgets = "widgets";
    std::string empty = "";
    std::uint64_t key;
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        std::uint64_t key;
        for (int i = 0; i < 1000; i++) {
            key = i;
            if (auto err = b->Put(u64tob(key), to_bytes(empty)); err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto b = tx->Bucket(to_bytes(widgets));
        for (int i = 0; i < 600; i++) {
            key = i;
            if (auto err = b->Delete(u64tob(key)); err != bolt::Success) {
                return err;
            }
        }

        auto c = b->Cursor();
        int n = 0;
        for (auto [k, v] = c->First(); !k.empty(); std::tie(k, std::ignore) = c->Next()) {
            n++;
        }
        if (n != 400) {
            fmt::println("unexpected key count: {}", n);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_QuickCheck() {
    QuickCheck qc;
    auto fn = [](TestData &testdata) -> bool {
        std::string widgets = "widgets";
        bolt::impl::BucketPtr b;
        auto db = MustOpenDB();
        auto [tx, err] = db->Begin(true);
        if (err != bolt::Success) {
            fmt::println("Begin tx fail, {}", err);
            return false;
        }
        std::tie(b, err) = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            fmt::println("CreateBucket fail, {}", err);
        }
        for (auto &[k, v] : testdata) {
            if (err = b->Put(k, v); err != bolt::Success) {
                fmt::println("Put {{{}, {}}} fail, {}", k, v, err);
                return false;
            }
        }
        if (err = tx->Commit(); err != bolt::Success) {
            fmt::println("Commit fail, {}", err);
            return false;
        }

        testdata.Sort();

        size_t index = 0;
        std::tie(tx, err) = db->Begin(false);
        if (err != bolt::Success) {
            fmt::println("Begin tx fail, {}", err);
            return false;
        }
        auto c = tx->Bucket(to_bytes(widgets))->Cursor();
        for (auto [k, v] = c->First(); !k.empty(); std::tie(k, v) = c->Next()) {
            if (!Equal(k, testdata[index].first)) {
                fmt::println("unexpected key: {}, {}", k, testdata[index].first);
                return false;
            } else if (!Equal(v, testdata[index].second)) {
                fmt::println("unexpected value: {}", v);
                return false;
            }
            index++;
        }
        if (testdata.size() != index) {
            fmt::println("unexpected item count: {}, expected {}",
                         testdata.size(), index);
            return false;
        }
        if (err = tx->Rollback(); err != bolt::Success) {
            fmt::println("Rollback fail, {}", err);
            return false;
        }
        MustCloseDB(std::move(db));
        return true;
    };
    if (auto ret = qc.Check(fn, 100); !ret) {
        return TestResult(false, "quick check fail");
    }
    return true;
}

TestResult TestCursor_QuickCheck_Reverse() {
    auto db = MustOpenDB();

    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_QuickCheck_BucketsOnly() {
    auto db = MustOpenDB();

    MustCloseDB(std::move(db));
    return true;
}

TestResult TestCursor_QuickCheck_BucketsOnly_Reverse() {
    auto db = MustOpenDB();

    MustCloseDB(std::move(db));
    return true;
}
