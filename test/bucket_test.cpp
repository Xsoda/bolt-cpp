#include "bolt/common.hpp"
#include "bolt/error.hpp"
#include "impl/file.hpp"
#include "impl/utils.hpp"
#include "impl/cursor.hpp"
#include "test.hpp"
#include "util.hpp"
#include "quick_check.hpp"

TestResult TestBucket_Get_NonExistent() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (auto v = b->Get(to_bytes(foo)); !v.empty()) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Get_FromNode() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(bar)); err != bolt::Success) {
            return err;
        }
        if (auto v = b->Get(to_bytes(foo)); !Equal(v, to_bytes(bar))) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBUcket_Get_IncompatibleValue() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        if (auto [b, err] =
                tx->Bucket(to_bytes(widgets))->CreateBucket(to_bytes(foo));
            err != bolt::Success) {
            return err;
        }
        if (auto v = tx->Bucket(to_bytes(widgets))->Get(to_bytes(foo));
            !v.empty()) {
            fmt::println("expected nil value");
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Get_Capacity() {
    // NOT NEED
    return true;
}

TestResult TestBucket_Put() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(bar)); err != bolt::Success) {
            return err;
        }
        auto v = tx->Bucket(to_bytes(widgets))->Get(to_bytes(foo));
        if (!Equal(to_bytes(bar), v)) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_Repeat() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string baz = "baz";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(bar)); err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(baz)); err != bolt::Success) {
            return err;
        }
        auto v = tx->Bucket(to_bytes(widgets))->Get(to_bytes(foo));
        if (!Equal(to_bytes(baz), v)) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_Large() {
    auto db = MustOpenDB();
    auto count = 100;
    auto factor = 200;
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::vector<std::byte> key, val;
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        for (auto i = 1; i < count; i++) {
            key.assign(i * factor, std::byte('0'));
            val.assign((count - i) * factor, std::byte('X'));
            if (auto err = b->Put(key, val); err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::vector<std::byte> key, val;
        auto b = tx->Bucket(to_bytes(widgets));
        for (auto i = 1; i < count; i++) {
            key.assign(i * factor, std::byte('0'));
            val.assign((count - i) * factor, std::byte('X'));
            auto value = b->Get(key);
            if (!Equal(value, val)) {
                fmt::println("unexpected value: {}", value);
                return bolt::ErrorUnexpected;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "view verify fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_VeryLarge() {
    auto db = MustOpenDB();
    auto n = 400000;
    auto batchN = 200000;
    auto ksize = 8;
    auto vsize = 500;
    db->StrictMode = false;
    for (auto i = 0; i < n; i += batchN) {
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            std::string widgets = "widgets";
            auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(widgets));
            if (err != bolt::Success) {
                return err;
            }
            std::vector<std::byte> key, val;
            key.assign(ksize, std::byte(0));
            val.assign(vsize, std::byte(0));
            for (auto j = 0; j < batchN; j++) {
                std::uint32_t k = i + j;
                if constexpr (std::endian::native == std::endian::little) {
                    k = byteswap(k);
                }
                *reinterpret_cast<std::uint32_t *>(key.data()) = k;
                if (auto err = b->Put(key, val); err != bolt::Success) {
                    return err;
                }
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            return TestResult(false, "update fail, {}", err);
        }
    }
    MustCheck(db);
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBUcket_Put_IncompatibleValue() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        if (auto [b, err] =
            tx->Bucket(to_bytes(widgets))->CreateBucket(to_bytes(foo));
            err != bolt::Success) {
            return err;
        }
        if (auto err = tx->Bucket(to_bytes(widgets))
            ->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::ErrorIncompatiableValue) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "udpate fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_Closed() {
    bolt::impl::BucketPtr b;
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    std::string widgets = "widgets";
    std::string foo = "foo";
    std::string bar = "bar";
    if (err != bolt::Success) {
        return TestResult(false, "Begin tx fail, {}", err);
    }
    std::tie(b, err) = tx->CreateBucket(to_bytes(widgets));
    if (err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "Rollback fail, {}", err);
    }
    if (err = b->Put(to_bytes(foo), to_bytes(bar));
        err != bolt::ErrorTxClosed) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_ReadOnly() {
    std::string widgets = "widgets";
    std::string foo = "foo";
    std::string bar = "bar";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
              err != bolt::Success) {
              return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto b = tx->Bucket(to_bytes(widgets));
        if (auto err = b->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::ErrorTxNotWritable) {
            fmt::println("unexpected error: {}", err);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Delete() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(bar)); err != bolt::Success) {
            return err;
        }
        if (err = b->Delete(to_bytes(foo)); err != bolt::Success) {
            return err;
        }
        if (auto v = b->Get(to_bytes(foo)); !v.empty()) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Delete_Large() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        std::vector<std::byte> val;
        val.assign(1024, std::byte('*'));
        for (auto i = 0; i < 100; i++) {
            auto key = fmt::format("{}", i);
            if (err = b->Put(to_bytes(key), val); err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        auto b = tx->Bucket(to_bytes(widgets));
        for (auto i = 0; i < 100; i++) {
            auto key = fmt::format("{}", i);
            if (auto err = b->Delete(to_bytes(key)); err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fil, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Delete_FreelistOverflow() {
    auto db = MustOpenDB();
    std::vector<std::byte> key;
    std::vector<std::byte> val;
    key.assign(16, std::byte(0));
    db->StrictMode = false;
    for (auto i = std::uint64_t(0); i < 10000; i++) {
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            std::string zero = "0";
            auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(zero));
            if (err != bolt::Success) {
                return err;
            }
            for (auto j = std::uint64_t(0); j < 1000; j++) {
                auto k = reinterpret_cast<std::uint64_t *>(key.data());
                std::uint64_t v = i;
                if constexpr (std::endian::native == std::endian::little) {
                    v = byteswap(v);
                }
                k[0] = v;
                v = j;
                if constexpr (std::endian::native == std::endian::little) {
                    v = byteswap(v);
                }
                k[1] = v;

                if (err = b->Put(key, val); err != bolt::Success) {
                    return err;
                }
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            return TestResult(false, "Update fail, {}", err);
        }
        if (i % 100 == 0) {
            fmt::println("round {:04}", i);
        }
    }
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string zero = "0";
        auto b = tx->Bucket(to_bytes(zero));
        auto c = b->Cursor();
        for (auto [k, v] = c->First(); !k.empty(); std::tie(k, v) = c->Next()) {
            if (auto err = c->Delete(); err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCheck(db);
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Nested() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string zero = "0000";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        std::tie(std::ignore, err) = b->CreateBucket(to_bytes(foo));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(bar), to_bytes(zero)); err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCheck(db);
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string bar = "bar";
        std::string xxxx = "xxxx";
        auto b = tx->Bucket(to_bytes(widgets));
        if (auto err = b->Put(to_bytes(bar), to_bytes(xxxx));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCheck(db);
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        auto b = tx->Bucket(to_bytes(widgets));
        for (auto i = 0; i < 10000; i++) {
            auto key = fmt::format("{}", i);
            if (auto err = b->Put(to_bytes(key), to_bytes(key));
                err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCheck(db);
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string baz = "baz";
        std::string yyyy = "yyyy";
        auto b = tx->Bucket(to_bytes(widgets));
        if (auto err = b->RetrieveBucket(to_bytes(foo))
                           ->Put(to_bytes(baz), to_bytes(yyyy));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCheck(db);
    // verify
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string baz = "baz";
        std::string xxxx = "xxxx";
        std::string yyyy = "yyyy";
        auto b = tx->Bucket(to_bytes(widgets));
        if (auto v = b->RetrieveBucket(to_bytes(foo))->Get(to_bytes(baz));
            !Equal(v, to_bytes(yyyy))) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        if (auto v = b->Get(to_bytes(bar)); !Equal(v, to_bytes(xxxx))) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        for (auto i = 0; i < 10000; i++) {
            auto key = fmt::format("{}", i);
            if (auto v = b->Get(to_bytes(key)); !Equal(v, to_bytes(key))) {
                fmt::println("unexpected value: {}", v);
                return bolt::ErrorUnexpected;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "view fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Delete_Bucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (std::tie(std::ignore, err) = b->CreateBucket(to_bytes(foo));
            err != bolt::Success) {
            return err;
        }
        if (err = b->Delete(to_bytes(foo));
            err != bolt::ErrorIncompatiableValue) {
            fmt::println("unexpected error: {}", err);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Delete_ReadOnly() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        if (auto err = tx->Bucket(to_bytes(widgets))->Delete(to_bytes(foo));
            err != bolt::ErrorTxNotWritable) {
            fmt::println("unexpected error: {}", err);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "view fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Delete_Closed() {
    auto db = MustOpenDB();
    std::string widgets = "widgets";
    std::string foo = "foo";
    bolt::impl::BucketPtr b;
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin tx fail, {}", err);
    }
    std::tie(b, err) = tx->CreateBucket(to_bytes(widgets));
    if (err != bolt::Success) {
        return TestResult(false, "CreateBucket fail, {}", err);
    }
    if (err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "Rollback fail, {}", err);
    }
    if (err = b->Delete(to_bytes(foo)); err != bolt::ErrorTxClosed) {
        return TestResult(false, "Unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_DeleteBucket_Nested() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string bat = "bat";
        std::string baz = "baz";
        bolt::impl::BucketPtr f, b;
        auto [w, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        std::tie(f, err) = w->CreateBucket(to_bytes(foo));
        if (err != bolt::Success) {
            return err;
        }
        std::tie(b, err) = f->CreateBucket(to_bytes(bar));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(baz), to_bytes(bat)); err != bolt::Success) {
            return err;
        }
        if (err = tx->Bucket(to_bytes(widgets))->DeleteBucket(to_bytes(foo));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_DeleteBucket_Nested2() {
    std::string widgets = "widgets";
    std::string foo = "foo";
    std::string bar = "bar";
    std::string bat = "bat";
    std::string baz = "baz";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        bolt::ErrorCode err;
        bolt::impl::BucketPtr w, f, b;
        if (std::tie(w, err) = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        if (std::tie(f, err) = w->CreateBucket(to_bytes(foo));
            err != bolt::Success) {
            return err;
        }
        if (std::tie(b, err) = f->CreateBucket(to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(baz), to_bytes(bat)); err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        bolt::ErrorCode err;
        bolt::impl::BucketPtr w, f, b;
        if (w = tx->Bucket(to_bytes(widgets)); w == nullptr) {
            fmt::println("{} bucket not found", widgets);
            return bolt::ErrorUnexpected;
        }
        if (f = w->RetrieveBucket(to_bytes(foo)); f == nullptr) {
            fmt::println("{} bucket not found", foo);
            return bolt::ErrorUnexpected;
        }
        if (b = f->RetrieveBucket(to_bytes(bar)); b == nullptr) {
            fmt::println("{} bucket not found", bar);
            return bolt::ErrorUnexpected;
        }
        if (auto v = b->Get(to_bytes(baz)); !Equal(v, to_bytes(bat))) {
            fmt::println("unexpected value: {}", v);
            return bolt::ErrorUnexpected;
        }
        if (err = tx->DeleteBucket(to_bytes(widgets)); err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        if (tx->Bucket(to_bytes(widgets)) != nullptr) {
            fmt::println("expected bucket to be deleted");
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

TestResult TestBucket_DeleteBucket_Large() {
    auto db = MustOpenDB();
    std::string widgets = "widgets";
    std::string foo = "foo";
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        bolt::ErrorCode err;
        bolt::impl::BucketPtr w, f;
        if (std::tie(w, err) = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        if (std::tie(f, err) = w->CreateBucket(to_bytes(foo));
            err != bolt::Success) {
            return err;
        }
        for (auto i = 0; i < 1000; i++) {
            auto key = fmt::format("{}", i);
            auto val = fmt::format("{:0100}", i);
            if (err = f->Put(to_bytes(key), to_bytes(val));
                err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          if (auto err = tx->DeleteBucket(to_bytes(widgets));
              err != bolt::Success) {
              return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}


TestResult TestBucket_Bucket_IncompatibleValue() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        auto [wid, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = wid->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        if (auto b =
                tx->Bucket(to_bytes(widgets))->RetrieveBucket(to_bytes(foo));
            b != nullptr) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "udpate fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_CreateBucket_IncompatibleValue() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";

        auto [wid, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = wid->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        if (std::tie(std::ignore, err) = wid->CreateBucket(to_bytes(foo));
            err != bolt::ErrorIncompatiableValue) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_DeleteBucket_IncompatibleValue() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        auto [wid, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = wid->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        if (err = tx->Bucket(to_bytes(widgets))->DeleteBucket(to_bytes(foo));
            err != bolt::ErrorIncompatiableValue) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Sequence() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string zero = "0";
        auto [bkt, err] = tx->CreateBucket(to_bytes(zero));
        if (err != bolt::Success) {
            return err;
        }
        if (auto v = bkt->Sequence(); v != 0) {
            fmt::println("unexpected sequence: {}", v);
            return bolt::ErrorUnexpected;
        }
        if (err = bkt->SetSequence(1000); err != bolt::Success) {
            return err;
        }
        if (auto v = bkt->Sequence(); v != 1000) {
            fmt::println("unexpected sequence: {}", v);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string zero = "0";
        if (auto v = tx->Bucket(to_bytes(zero))->Sequence(); v != 1000) {
            fmt::println("unexpected sequence: {}", v);
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

TestResult TestBucket_NextSequence() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string woojits = "woojits";
        bolt::impl::BucketPtr wid, woo;
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        } else {
            wid = b;
        }
        if (auto [b, err] = tx->CreateBucket(to_bytes(woojits));
            err != bolt::Success) {
            return err;
        } else {
            woo = b;
        }
        if (auto [seq, err] = wid->NextSequence(); err != bolt::Success) {
            return err;
        } else if (seq != 1) {
            fmt::println("unexpcted sequence: {}", seq);
            return bolt::ErrorUnexpected;
        }
        if (auto [seq, err] = wid->NextSequence(); err != bolt::Success) {
            return err;
        } else if (seq != 2) {
            fmt::println("unexpected sequence: {}", seq);
            return bolt::ErrorUnexpected;
        }
        if (auto [seq, err] = woo->NextSequence(); err != bolt::Success) {
            return err;
        } else if (seq != 1) {
            fmt::println("unexpected sequence: {}", seq);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_NextSequence_Persist() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [seq, err] = tx->Bucket(to_bytes(widgets))->NextSequence();
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          auto [seq, err] = tx->Bucket(to_bytes(widgets))->NextSequence();
          if (err != bolt::Success) {
              fmt::println("unexpected error: {}", err);
              return err;
          } else if (seq != 2) {
              fmt::println("unexpected sequence: {}", seq);
              return bolt::ErrorUnexpected;
          }
          return bolt::Success;
        });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_NextSequence_ReadOnly() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "unexpected error, {}", err);
    }
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        auto [seq, err] = tx->Bucket(to_bytes(widgets))->NextSequence();
        if (err != bolt::ErrorTxNotWritable) {
            fmt::println("unexpected error: {}", err);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "view fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_NextSequence_Closed() {
    auto db = MustOpenDB();
    std::string widgets = "widgets";
    bolt::impl::BucketPtr b;
    std::uint64_t sequence;
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx fail, {}", err);
    }
    std::tie(b, err) = tx->CreateBucket(to_bytes(widgets));
    if (err != bolt::Success) {
        return TestResult(false, "CreateBucket fail, {}", err);
    }
    if (err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "Rollback fail, {}", err);
    }
    if (std::tie(sequence, err) = b->NextSequence();
        err != bolt::ErrorTxClosed) {
        return TestResult(false, "unexpected error, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
  }

TestResult TestBucket_ForEach() {
    std::string widgets = "widgets";
    std::string bar = "bar";
    std::string baz = "baz";
    std::string foo = "foo";
    std::string zero = "0000";
    std::string one = "0001";
    std::string two = "0002";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(zero)); err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(baz), to_bytes(one)); err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(bar), to_bytes(two)); err != bolt::Success) {
            return err;
        }
        auto index = 0;
        if (auto err = b->ForEach([&](bolt::const_bytes k,
                                      bolt::const_bytes v) -> bolt::ErrorCode {
            switch (index) {
            case 0:
                if (!Equal(k, to_bytes(bar))) {
                    fmt::println("unexpected key: {}", k);
                    return bolt::ErrorUnexpected;
                } else if (!Equal(v, to_bytes(two))) {
                    fmt::println("unexpected value: {}", v);
                    return bolt::ErrorUnexpected;
                }
                break;
            case 1:
                if (!Equal(k, to_bytes(baz))) {
                    fmt::println("unexpected key: {}", k);
                    return bolt::ErrorUnexpected;
                } else if (!Equal(v, to_bytes(one))) {
                    fmt::println("unexpected value: {}", v);
                    return bolt::ErrorUnexpected;
                }
                break;
            case 2:
                if (!Equal(k, to_bytes(foo))) {
                    fmt::println("unexpected key: {}", k);
                    return bolt::ErrorUnexpected;
                } else if (!Equal(v, to_bytes(zero))) {
                    fmt::println("unexpected value: {}", v);
                    return bolt::ErrorUnexpected;
                }
                break;
            }
            index++;
            return bolt::Success;
        });
            err != bolt::Success) {
            return err;
        }
        if (index != 3) {
            fmt::println("unexpected index: {}", index);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_ForEach_ShortCircuit() {
    std::string widgets = "widgets";
    std::string bar = "bar";
    std::string baz = "baz";
    std::string foo = "foo";
    std::string zeros = "0000";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(bar), to_bytes(zeros));
            err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(baz), to_bytes(zeros));
            err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(zeros));
            err != bolt::Success) {
            return err;
        }
        int index = 0;
        if (err = tx->Bucket(to_bytes(widgets))
            ->ForEach([&](bolt::const_bytes k,
                          bolt::const_bytes v) -> bolt::ErrorCode {
                index++;
                if (Equal(k, to_bytes(baz))) {
                    return bolt::ErrorExpected;
                }
                return bolt::Success;
            });
            err == bolt::Success || err != bolt::ErrorExpected) {
            return bolt::ErrorUnexpected;
        }
        if (index != 2) {
            fmt::println("unexpected index: {}", index);
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_ForEach_Closed() {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx fail, {}", err);
    }
    bolt::impl::BucketPtr b;
    std::string widgets = "widgets";
    std::tie(b, err) = tx->CreateBucket(to_bytes(widgets));
    if (err != bolt::Success) {
        return TestResult(false, "CreateBucket fail, {}", err);
    }
    if (err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "Rollack fail, {}", err);
    }
    if (err = b->ForEach([](bolt::const_bytes k, bolt::const_bytes v)
                         -> bolt::ErrorCode { return bolt::Success; });
        err != bolt::ErrorTxClosed) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_EmptyKey() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string bar = "bar";
        std::string empty = "";
        bolt::bytes zero;
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(empty), to_bytes(bar));
            err != bolt::ErrorKeyRequired) {
            return bolt::ErrorUnexpected;
        }
        if (err = b->Put(zero, to_bytes(bar)); err != bolt::ErrorKeyRequired) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_KeyTooLarge() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string bar = "bar";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        auto key = std::span<std::byte>(
            reinterpret_cast<std::byte *>(bar.data()), 32769);
        if (err = b->Put(key, to_bytes(bar)); err != bolt::ErrorKeyTooLarge) {
          return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
  }
  MustCloseDB(std::move(db));
  return true;
}

TestResult TestBucket_Put_ValueTooLarge() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        auto val = std::span<std::byte>(
            reinterpret_cast<std::byte *>(foo.data()), bolt::MaxValueSize + 1);
        if (err = b->Put(to_bytes(foo), val); err != bolt::ErrorValueTooLarge) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Stats() {
    auto db = MustOpenDB();
    std::string bigKey = "really-big-value";
    for (int i = 0; i < 500; i++) {
        if (auto err = db->Update([&, i](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            std::string woojits = "woojits";
            auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(woojits));
            if (err != bolt::Success) {
                return err;
            }
            auto key = fmt::format("{:03}", i);
            auto val = fmt::format("{}", i);
            if (err = b->Put(to_bytes(key), to_bytes(val));
                err != bolt::Success) {
                return err;
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            return TestResult(false, "Update fail, {}", err);
        }
    }
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string woojits = "woojits";
        std::vector<std::byte> bigValue;
        bigValue.assign(10000, std::byte('*'));
        auto b = tx->Bucket(to_bytes(woojits));
        if (auto err = b->Put(to_bytes(bigKey), to_bytes(bigValue));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }

    MustCheck(db);

    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "woojits";
        auto b = tx->Bucket(to_bytes(name));
        auto stats = b->Stats();
        auto branchInuse = 16;
        branchInuse += 7 * 16;
        branchInuse += 7 * 3;

        size_t leafInuse = 7 * 16;
        leafInuse += 501 * 16;
        leafInuse += 500 * 3 + bigKey.size();
        leafInuse += 1 * 10 + 2 * 90 + 3 * 400 + 10000;
        if (stats.BranchPageN != 1) {
            fmt::println("unexpected BranchPageN: {}", stats.BranchPageN);
        } else if (stats.BranchOverflowN != 0) {
            fmt::println("unexpected BranchOverflowN: {}", stats.BranchOverflowN);
        } else if (stats.LeafPageN != 7) {
            fmt::println("unexpected LeafPageN: {}", stats.LeafPageN);
        } else if (stats.LeafOverflowN != 2) {
            fmt::println("unexpected LeafOverflowN: {}", stats.LeafOverflowN);
        } else if (stats.KeyN != 501) {
            fmt::println("unexpected KeyN: {}", stats.KeyN);
        } else if (stats.Depth != 2) {
            fmt::println("unexpected Depth: {}", stats.Depth);
        } else if (stats.BranchInuse != branchInuse) {
            fmt::println("unexpected BranchInuse: {}", stats.BranchInuse);
        } else if (stats.LeafInuse != leafInuse) {
            fmt::println("unexpected LeafInuse: {}", stats.LeafInuse);
        }
        if (bolt::impl::Getpagesize() == 4096) {
            if (stats.BranchAlloc != 4096) {
                fmt::println("unexpected BranchAlloc: {}", stats.BranchAlloc);
            } else if (stats.LeafAlloc != 36864) {
                fmt::println("unexpected LeafAlloc: {}", stats.LeafAlloc);
            }
        }
        if (stats.BucketN != 1) {
            fmt::println("unexpected BucketN: {}", stats.BucketN);
        } else if (stats.InlineBucketN != 0) {
            fmt::println("unexpected InlineBucketN: {}", stats.InlineBucketN);
        } else if (stats.InlineBucketInuse != 0) {
            fmt::println("unexpected InlineBucketInuse: {}", stats.InlineBucketInuse);
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true; }

TestResult TestBucket_Stats_RandomFill() { return true; }

TestResult TestBucket_Stats_Small() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string whozawhats = "whozawhats";
        std::string foo = "foo", bar = "bar";
        auto [b, err] = tx->CreateBucket(to_bytes(whozawhats));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(bar)); err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCheck(db);
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "whozawhats";
        auto b = tx->Bucket(to_bytes(name));
        auto stats = b->Stats();
        if (stats.BranchPageN != 0) {
            fmt::println("unexpected BranchPageN: {}", stats.BranchPageN);
        } else if (stats.BranchOverflowN != 0) {
            fmt::println("unexpected BranchOverflowN: {}", stats.BranchOverflowN);
        } else if (stats.LeafPageN != 0) {
            fmt::println("unexpected LeafPageN: {}", stats.LeafPageN);
        } else if (stats.LeafOverflowN != 0) {
            fmt::println("unexpected LeafOverflowN: {}", stats.LeafOverflowN);
        } else if (stats.KeyN != 1) {
            fmt::println("unexpected KeyN: {}", stats.KeyN);
        } else if (stats.Depth != 1) {
            fmt::println("unexpected Depth: {}", stats.Depth);
        } else if (stats.BranchInuse != 0) {
            fmt::println("unexpected BranchInuse: {}", stats.BranchInuse);
        } else if (stats.LeafInuse != 0) {
            fmt::println("unexpected LeafInuse: {}", stats.LeafInuse);
        }
        if (bolt::impl::Getpagesize() == 4096) {
            if (stats.BranchAlloc != 0) {
                fmt::println("unexpected BranchAlloc: {}", stats.BranchAlloc);
            } else if (stats.LeafAlloc != 0) {
                fmt::println("unexpected LeafAlloc: {}", stats.LeafAlloc);
            }
        }
        if (stats.BucketN != 1) {
            fmt::println("unexpected BucketN: {}", stats.BucketN);
        } else if (stats.InlineBucketN != 1) {
            fmt::println("unexpected InlineBucketN: {}", stats.InlineBucketN);
        } else if (stats.InlineBucketInuse != 16 + 16 + 6) {
            fmt::println("unexpected InlineBucketInuse: {}", stats.InlineBucketInuse);
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Stats_EmptyBucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "whozawhats";
        if (auto [b, err] = tx->CreateBucket(to_bytes(name));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail, {}", err);
    }
    MustCheck(db);
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "whozawhats";
        auto b = tx->Bucket(to_bytes(name));
        auto stats = b->Stats();
        if (stats.BranchPageN != 0) {
            fmt::println("unexpected BranchPageN: {}", stats.BranchPageN);
        } else if (stats.BranchOverflowN != 0) {
            fmt::println("unexpected BranchOverflowN: {}", stats.BranchOverflowN);
        } else if (stats.LeafPageN != 0) {
            fmt::println("unexpected LeafPageN: {}", stats.LeafPageN);
        } else if (stats.LeafOverflowN != 0) {
            fmt::println("unexpected LeafOverflowN: {}", stats.LeafOverflowN);
        } else if (stats.KeyN != 0) {
            fmt::println("unexpected KeyN: {}", stats.KeyN);
        } else if (stats.Depth != 1) {
            fmt::println("unexpected Depth: {}", stats.Depth);
        } else if (stats.BranchInuse != 0) {
            fmt::println("unexpected BranchInuse: {}", stats.BranchInuse);
        } else if (stats.LeafInuse != 0) {
            fmt::println("unexpected LeafInuse: {}", stats.LeafInuse);
        }
        if (bolt::impl::Getpagesize() == 4096) {
            if (stats.BranchAlloc != 0) {
                fmt::println("unexpected BranchAlloc: {}", stats.BranchAlloc);
            } else if (stats.LeafAlloc != 0) {
                fmt::println("unexpected LeafAlloc: {}", stats.LeafAlloc);
            }
        }
        if (stats.BucketN != 1) {
            fmt::println("unexpected BucketN: {}", stats.BucketN);
        } else if (stats.InlineBucketN != 1) {
            fmt::println("unexpected InlineBucketN: {}", stats.InlineBucketN);
        } else if (stats.InlineBucketInuse != 16) {
            fmt::println("unexpected InlineBucketInuse: {}", stats.InlineBucketInuse);
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Stats_Nested() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string foo = "foo";
        std::string bar = "bar";
        std::string baz = "baz";
        bolt::impl::BucketPtr br, bz;
        auto [b, err] = tx->CreateBucket(to_bytes(foo));
        if (err != bolt::Success) {
            return err;
        }
        for (int i = 0; i < 100; i++) {
            auto val = fmt::format("{:02}", i);
            if (auto err = b->Put(to_bytes(val), to_bytes(val));
                err != bolt::Success) {
                return err;
            }
        }

        std::tie(br, err) = b->CreateBucket(to_bytes(bar));
        if (err != bolt::Success) {
            return err;
        }
        for (int i = 0; i < 10; i++) {
            auto val = fmt::format("{}", i);
            if (auto err = br->Put(to_bytes(val), to_bytes(val));
                err != bolt::Success) {
                return err;
            }
        }

        std::tie(bz, err) = br->CreateBucket(to_bytes(baz));
        if (err != bolt::Success) {
            return err;
        }
        for (int i = 0; i < 10; i++) {
            auto val = fmt::format("{}", i);
            if (auto err = bz->Put(to_bytes(val), to_bytes(val));
                err != bolt::Success) {
                return err;
            }
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }

    MustCheck(db);
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string foo = "foo";
        auto b = tx->Bucket(to_bytes(foo));
        auto stats = b->Stats();
        if (stats.BranchPageN != 0) {
            fmt::println("unexpected BranchPageN: {}", stats.BranchPageN);
        } else if (stats.BranchOverflowN != 0) {
            fmt::println("unexpected BranchOverflowN: {}", stats.BranchOverflowN);
        } else if (stats.LeafPageN != 2) {
            fmt::println("unexpected LeafPageN: {}", stats.LeafPageN);
        } else if (stats.LeafOverflowN != 0) {
            fmt::println("unexpected LeafOverflowN: {}", stats.LeafOverflowN);
        } else if (stats.KeyN != 122) {
            fmt::println("unexpected KeyN: {}", stats.KeyN);
        } else if (stats.Depth != 3) {
            fmt::println("unexpected Depth: {}", stats.Depth);
        } else if (stats.BranchInuse != 0) {
            fmt::println("unexpected BranchInuse: {}", stats.BranchInuse);
        }

        auto sfoo = 16;
        sfoo += 101 * 16;
        sfoo += 100 * 2 + 100 * 2;
        sfoo += 3 + 16;

        auto sbar = 16;
        sbar += 11 * 16;
        sbar += 10 + 10;
        sbar += 3 + 16;

        auto sbaz = 16;
        sbaz += 10 * 16;
        sbaz += 10 + 10;
        if (stats.LeafInuse != sfoo + sbar + sbaz) {
            fmt::println("unexpected LeafInuse: {}", stats.LeafInuse);
        }

        if (bolt::impl::Getpagesize() == 4096) {
            if (stats.BranchAlloc != 0) {
                fmt::println("unexpected BranchAlloc: {}", stats.BranchAlloc);
            } else if (stats.LeafAlloc != 8192) {
                fmt::println("unexpected LeafAlloc: {}", stats.LeafAlloc);
            }
        }

        if (stats.BucketN != 3) {
            fmt::println("unexpected BucketN: {}", stats.BucketN);
        } else if (stats.InlineBucketN != 1) {
            fmt::println("unexpected InlineBucketN: {}", stats.InlineBucketN);
        } else if (stats.InlineBucketInuse != sbaz) {
            fmt::println("unexpected InlineBucketInuse: {}", stats.InlineBucketInuse);
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "view fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Stats_Large() {
    auto db = MustOpenDB();
    int index = 0;
    for (int i = 0; i < 100; i++) {
        if (auto err = db->Update([&index](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            std::string widgets = "widgets";
            auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(widgets));
            if (err != bolt::Success) {
                    return err;
            }
            for (int j = 0; j < 1000; j++) {
                auto val = fmt::format("{}", index);
                if (auto err = b->Put(to_bytes(val), to_bytes(val)); err != bolt::Success) {
                    return err;
                }
                index++;
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            return TestResult(false, "Update fail, {}", err);
        }
    }

    MustCheck(db);

    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        auto stats = tx->Bucket(to_bytes(widgets))->Stats();
        if (stats.BranchPageN != 13) {
            fmt::println("unexpected BranchPageN: {}", stats.BranchPageN);
        } else if (stats.BranchOverflowN != 0) {
            fmt::println("unexpected BranchOverflowN: {}", stats.BranchOverflowN);
        } else if (stats.LeafPageN != 1196) {
            fmt::println("unexpected LeafPageN: {}", stats.LeafPageN);
        } else if (stats.LeafOverflowN != 0) {
            fmt::println("unexpected LeafOverflowN: {}", stats.LeafOverflowN);
        } else if (stats.KeyN != 100000) {
            fmt::println("unexpected KeyN: {}", stats.KeyN);
        } else if (stats.Depth != 3) {
            fmt::println("unexpected Depth: {}", stats.Depth);
        } else if (stats.BranchInuse != 25257) {
            fmt::println("unexpected BranchInuse: {}", stats.BranchInuse);
        } else if (stats.LeafInuse != 2596916) {
            fmt::println("unexpected LeafInuse: {}", stats.LeafInuse);
        }
        if (bolt::impl::Getpagesize() == 4096) {
            if (stats.BranchAlloc != 53248) {
                fmt::println("unexpected BranchAlloc: {}", stats.BranchAlloc);
            } else if (stats.LeafAlloc != 4898816) {
                fmt::println("unexpected LeafAlloc: {}", stats.LeafAlloc);
            }
        }
        if (stats.BucketN != 1) {
            fmt::println("unexpected BucketN: {}", stats.BucketN);
        } else if (stats.InlineBucketN != 0) {
            fmt::println("unexpected InlineBucketN: {}", stats.InlineBucketN);
        } else if (stats.InlineBucketInuse != 0) {
            fmt::println("unexpected InlineBucketInuse: {}", stats.InlineBucketInuse);
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "view fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestBucket_Put_Single() {
    QuickCheck qc;
    auto func = [](TestData &testdata) -> bool {
        auto db = MustOpenDB();
        std::string widgets = "widgets";
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
                err != bolt::Success) {
                return err;
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            fmt::println("Update fail, {}", err);
            return false;
        }
        std::vector<std::pair<bolt::const_bytes, bolt::const_bytes>> keys;
        for (auto &[k, v] : testdata) {
            if (auto err =
                db->Update([&keys, &widgets, k = k, v = v](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
                    auto b = tx->Bucket(to_bytes(widgets));
                    if (auto err = b->Put(k, v); err != bolt::Success) {
                        return err;
                    }
                    keys.push_back(std::make_pair(k, v));
                    return bolt::Success;
                });
                err != bolt::Success) {
                fmt::println("put fail, {} => {}", k, v);
                return false;
            }
            if (auto err = db->View(
                    [&keys, &widgets](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
                        auto b = tx->Bucket(to_bytes(widgets));
                        for (auto &[kk, kv] : keys) {
                            auto value = b->Get(kk);
                            if (!Equal(value, kv)) {
                                fmt::println("value mismatch\n:key: {}\ngot:{}\nexp: {}", kk, value, kv);
                            }
                        }
                        return bolt::Success;
                    });
                err != bolt::Success) {
                fmt::println("view fail, {}", err);
                return false;
            }
        }
        MustCloseDB(std::move(db));
        return true;
    };
    if (!qc.Check(func)) {
        return TestResult(false, "quick check fail");
    }
    return true;
}

TestResult TestBucket_Put_Multiple() {
    QuickCheck qc;
    auto func = [](TestData &testdata) -> bool {
        auto db = MustOpenDB();
        std::string widgets = "widgets";
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
                err != bolt::Success) {
                return err;
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            fmt::println("Update fail, {}", err);
            return false;
        }
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            auto b = tx->Bucket(to_bytes(widgets));
            for (auto &[k, v] : testdata) {
                if (auto err = b->Put(k, v); err != bolt::Success) {
                    return err;
                }
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            fmt::println("update fail, {}", err);
            return false;
        }
        if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            auto b = tx->Bucket(to_bytes(widgets));
            for (auto &[k, v] : testdata) {
                auto value = b->Get(k);
                if (!Equal(value, v)) {
                    fmt::println("expect value: {}, got: {}", v, value);
                }
            }
            return bolt::Success;
        });
            err != bolt::Success) {
            fmt::println("view fail, {}", err);
            return false;
        }
        MustCloseDB(std::move(db));
        return true;
    };
    if (!qc.Check(func)) {
        return TestResult(false, "quick check fail");
    }
    return true;
}

TestResult TestBucket_Delete_Quick() {
    QuickCheck qc;
    auto func = [](TestData &testdata) -> bool {
        auto db = MustOpenDB();
        fmt::println("database path: {}", db->Path());
        std::string widgets = "widgets";
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
                err != bolt::Success) {
                return err;
            }
            return bolt::Success;
        }); err != bolt::Success) {
            fmt::println(stderr, "CreateBucket fail, {}", err);
            return false;
        }
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
              auto b = tx->Bucket(to_bytes(widgets));
              for (auto &[k, v] : testdata) {
                  if (auto err = b->Put(k, v);
                      err != bolt::Success) {
                      return err;
                  }
              }
              return bolt::Success;
        }); err != bolt::Success) {
            return false;
        }

        auto count = 0;
        for (auto &[k, v] : testdata) {
            if (auto err = db->Update(
                    [&, k = k](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
                        auto b = tx->Bucket(to_bytes(widgets));
                        if (auto err = b->Delete(k);
                            err != bolt::Success) {
                            return err;
                        }
                        return bolt::Success;
                    }); err != bolt::Success) {
                fmt::println(stderr, "Update fail, {}", err);
                return false;
            }
        }
        if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
              if (auto err =
                      tx->Bucket(to_bytes(widgets))
                          ->ForEach([](bolt::const_bytes k,
                                       bolt::const_bytes v) -> bolt::ErrorCode {
                            fmt::println(stderr,
                                         "bucket should be empty, found {}", k);
                            return bolt::ErrorUnexpected;
                          });
                  err != bolt::Success) {
                return err;
              }
              return bolt::Success;
        }); err != bolt::Success) {
            fmt::println("View fail, {}", err);
            return false;
        }
        MustCloseDB(std::move(db));
        return true;
    };
    if (!qc.Check(func)) {
        return TestResult(false, "quick check fail");
    }
    return true;
}
