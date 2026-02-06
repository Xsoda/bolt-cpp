#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/file.hpp"
#include "impl/page.hpp"
#include "random.hpp"
#include "test.hpp"
#include "util.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <iostream>
#include <cassert>

using namespace std::chrono_literals;

TestResult TestDB_Open() {
    auto db = std::make_shared<bolt::impl::DB>();
    auto path = tempfile();
    auto err = db->Open(path, false);
    if (err != bolt::Success) {
        return TestResult(false, "expected open success");
    }
    auto s = db->Path();
    if (s != path) {
        return TestResult(false, "expected path");
    }
    err = db->Close();
    if (err != bolt::Success) {
        return TestResult(false, "expected close database");
    }
    return true;
}

TestResult TestDB_OpenPathRequired() {
    auto db = std::make_shared<bolt::impl::DB>();
    auto err = db->Open("", false);
    if (err == bolt::Success) {
        return TestResult(false, "expected open database fail");
    }
    return true;
}

TestResult TestDB_OpenInvalid() {
    auto path = tempfile();
    std::ofstream f{path, std::ios::binary | std::ios::out};
    std::string content = "this is not a bolt database";
    f.write(content.c_str(), content.size());
    f.close();

    auto db = std::make_shared<bolt::impl::DB>();
    auto err = db->Open(path, false);
    if (err != bolt::ErrorDatabaseInvalid) {
        return TestResult(false, "expected error is ErrorDatabaseInvalid");
    }
    return true;
}

TestResult TestDB_OpenErrVersionMismatch() {
    auto db = MustOpenDB();
    auto path = db->Path();
    MustCloseDB(std::move(db));

    bolt::impl::File file;
    std::uint64_t size;
    std::vector<std::byte> buf;
    auto err = file.Open(path, false);
    if (err != bolt::Success) {
        return TestResult(false, "open database file fail");
    }
    std::tie(size, err) = file.Size();
    if (err != bolt::Success) {
        return TestResult(false, "stat database file fail");
    }
    buf.assign(size, std::byte(0));
    std::tie(size, err) = file.ReadAt(buf, 0);
    if (err != bolt::Success) {
        return TestResult(false, "read database file fail");
    }
    auto meta0 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize]);
    meta0->version++;
    auto meta1 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize + bolt::impl::Getpagesize()]);
    meta1->version++;
    std::tie(size, err) = file.WriteAt(buf, 0);
    if (err != bolt::Success) {
        return TestResult(false, "write database file fail");
    }

    db = std::make_shared<bolt::impl::DB>();
    err = db->Open(path);
    if (err != bolt::ErrorVersionMismatch) {
        return TestResult(false, "expected error is ErrorVersionMismatch");
    }
    return true;
}

TestResult TestDB_OpenErrChecksum() {
    auto db = MustOpenDB();
    auto path = db->Path();
    MustCloseDB(std::move(db));

    bolt::impl::File file;
    std::uint64_t size;
    std::vector<std::byte> buf;
    auto err = file.Open(path, false);
    if (err != bolt::Success) {
        return TestResult(false, "open database file fail");
    }
    std::tie(size, err) = file.Size();
    if (err != bolt::Success) {
        return TestResult(false, "stat database file fail");
    }
    buf.assign(size, std::byte(0));
    std::tie(size, err) = file.ReadAt(buf, 0);
    if (err != bolt::Success) {
        return TestResult(false, "read database file fail");
    }
    auto meta0 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize]);
    meta0->pgid++;
    auto meta1 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize + bolt::impl::Getpagesize()]);
    meta1->pgid++;
    std::tie(size, err) = file.WriteAt(buf, 0);
    if (err != bolt::Success) {
        return TestResult(false, "write database file fail");
    }

    db = std::make_shared<bolt::impl::DB>();
    err = db->Open(path);
    if (err != bolt::ErrorChecksum) {
        return TestResult(false, "expected error is ErrorChecksum");
    }
    return true;
}

TestResult TestDB_OpenSize() {
    auto db = MustOpenDB();
    auto path = db->Path();

    auto pagesize = db->Info().PageSize;
    auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
      std::string name = "data";
      auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(name));
      if (err != bolt::Success) {
        return err;
      }
      std::vector<std::byte> value;
      value.assign(1000, std::byte(0));
      for (int i = 0; i < 10000; i++) {
        std::string key = fmt::format("{:04d}", i);
        err = b->Put(to_bytes(key), value);
        if (err != bolt::Success) {
          return err;
        }
      }
      return bolt::Success;
    });
    if (err != bolt::Success) {
        return TestResult(false, "database Update fail");
    }
    err = db->Close();
    if (err != bolt::Success) {
        return TestResult(false, "database close fail");
    }
    fmt::println("database {} closed", path);

    auto sz = std::filesystem::file_size(path);

    auto db0 = std::make_shared<bolt::impl::DB>();
    if (err = db0->Open(path); err != bolt::Success) {
        return TestResult(false, "reopen database {} fail, {}", path, err);
    }
    if (err = db0->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string data = "data";
        std::vector<std::byte> zero = {std::byte(0)};
        if (auto err =
            tx->Bucket(to_bytes(data))->Put(to_bytes(zero), to_bytes(zero));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (err = db0->Close(); err != bolt::Success) {
        return TestResult(false, "Close database fail, {}", err);
    }

    auto newSz = std::filesystem::file_size(path);
    if (newSz == 0) {
        return TestResult(false, "unexpected new file size: {}", newSz);
    }
    if (sz < newSz - 5 * pagesize) {
        return TestResult(false, "unexpected file growth: {} => {}", sz, newSz);
    }
    return true;
}

TestResult TestDB_Open_Size_Large() {
    auto db = MustOpenDB();
    auto path = db->Path();

    db->StrictMode = false;

    auto pagesize = db->Info().PageSize;
    std::uint64_t index = 0;
    std::vector<std::byte> value;
    std::string data = "data";
    value.assign(50, std::byte(0));
    auto zero = std::span<std::byte>(value.data(), 1);
    for (int i = 0; i < 10000 /* 10000 */; i++) {
        auto start = std::chrono::steady_clock::now();
        if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(data));
            for (int j = 0; j < 1000; j++) {
                if (err = b->Put(u64tob(index), value); err != bolt::Success) {
                    return err;
                }
                index++;
            }
            return bolt::Success;
        }); err != bolt::Success) {
            return TestResult(false, "Update fail, {}", err);
        }
        auto end = std::chrono::steady_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        fmt::println("{:04} | escaped: {:>6}", i, duration);
    }
    auto stats = db->Stats();
    fmt::println("     Rebalance: {:>6}", stats.TxStats.Rebalance);
    fmt::println(" RebalanceTime: {:>6}", stats.TxStats.RebalanceTime);
    fmt::println("         Spill: {:>6}", stats.TxStats.Spill);
    fmt::println("     SpillTime: {:>6}", stats.TxStats.SpillTime);
    fmt::println("         Write: {:>6}", stats.TxStats.Write);
    fmt::println("     WriteTime: {:>6}", stats.TxStats.WriteTime);
    MustCheck(db);
    MustCloseDB(std::move(db));

    auto sz = std::filesystem::file_size(path);
    if (sz == 0) {
        return TestResult(false, "unexpected new file size: {}", sz);
    } else if (sz < (1 << 30)) {
        return TestResult(false, "expected larger initial size: {}", sz);
    }

    db = MustOpenDB(path);
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        return tx->Bucket(to_bytes(data))->Put(zero, zero);
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    MustCloseDB(std::move(db));

    auto newSz = std::filesystem::file_size(path);
    if (newSz == 0) {
        return TestResult(false, "unexpected new file size: {}", newSz);
    }
    if (sz < newSz - 5 * pagesize) {
        return TestResult(false, "unexpected file growth: {} => {}", sz, newSz);
    }
    return true;
}

TestResult TestDB_Open_Check() {
    auto db = MustOpenDB();
    auto path = db->Path();
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto resultset = tx->Check().get();
        for (auto item : resultset) {
            fmt::println(" - {}", item);
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));

    db = MustOpenDB(path);
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto resultset = tx->Check().get();
        for (auto item : resultset) {
            fmt::println(" - {}", item);
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_Open_FileTooSmall() {
    auto db = MustOpenDB();
    auto path = db->Path();
    MustCloseDB(std::move(db));

    bolt::impl::File file;
    file.Open(path, false);
    file.Truncate(bolt::impl::Getpagesize());
    file.Close();

    db = std::make_shared<bolt::impl::DB>();
    auto err = db->Open(path);
    if (err == bolt::Success || err != bolt::ErrorFileSizeTooSmall) {
        return TestResult(false, "unexpected error: {}", err);
    }
    return true;
}

TestResult TestDB_Open_InitialMmapSize() {
    // NOT SUPPORT
    return true;
}

TestResult TestDB_Begin_ErrDatabaseNotOpen() {
    auto db = std::make_shared<bolt::impl::DB>();
    if (auto [tx, err] = db->Begin(true); err != bolt::ErrorDatabaseNotOpen) {
        return TestResult(false, "unexpected error, {}", err);
    }
    return true;
}

TestResult TestDB_BeginRW() {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin tx fail");
    } else if (tx == nullptr) {
        return TestResult(false, "expected tx");
    }
    if (tx->DB() != db) {
        return TestResult(false, "unexpected tx database");
    } else if (!tx->Writable()) {
        return TestResult(false, "expected writable tx");
    }
    if (err = tx->Commit(); err != bolt::Success) {
        return TestResult(false, "Commit tx fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_BeginRW_Closed() {
    auto db = std::make_shared<bolt::impl::DB>();
    if (auto [tx, err] = db->Begin(true); err != bolt::ErrorDatabaseNotOpen) {
        return TestResult(false, "unexpected error, {}", err);
    }
    return true;
}

TestResult Close_PendingTx(bool writable) {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin tx fail, {}", err);
    }
    auto done = std::async(std::launch::async, [db]() {
      if (auto err = db->Close(); err != bolt::Success) {
        fmt::println("close database fail, {}", err);
      }
    });
    auto status = done.wait_for(100ms);
    if (status == std::future_status::ready) {
        return TestResult(false, "database closed too early");
    }

    if (err = tx->Commit(); err != bolt::Success) {
        return TestResult(false, "Commit tx fail, {}", err);
    }

    status = done.wait_for(100ms);
    if (status != std::future_status::ready) {
        return TestResult(false, "database did not close");
    }
    MustCloseDB(std::move(db));
    return true; }


TestResult TestDB_Close_PendingTx_RW() { return Close_PendingTx(true); }

TestResult TestDB_Close_PendingTx_RO() { return Close_PendingTx(false); }

TestResult TestDB_Update() {
    std::string widgets = "widgets";
    std::string foo = "foo";
    std::string bar = "bar";
    std::string baz = "baz";
    std::string bat = "bat";
    auto db = MustOpenDB();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        if (err = b->Put(to_bytes(baz), to_bytes(bat)); err != bolt::Success) {
            return err;
        }
        if (err = b->Delete(to_bytes(foo)); err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          auto b = tx->Bucket(to_bytes(widgets));
          if (auto v = b->Get(to_bytes(foo)); !v.empty()) {
              fmt::println("expected nil value, got: {}", v);
              return bolt::ErrorUnexpected;
          }
          if (auto v = b->Get(to_bytes(baz)); !Equal(v, to_bytes(bat))) {
              fmt::println("unexpected value: {}", v);
              return bolt::ErrorUnexpected;
          }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "View fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_Update_Closed() {
    auto db = std::make_shared<bolt::impl::DB>();
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    }); err != bolt::ErrorDatabaseNotOpen) {
        return TestResult(false, "unexpected error: {}", err);
    }
    return true;
}

TestResult TestDB_Update_ManualCommit() {
    auto db = MustOpenDB();
    auto panicked = false;
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        try {
            if (auto err = tx->Commit(); err != bolt::Success) {
                return err;
            }
        } catch (const std::exception &e) {
            panicked = true;
            fmt::println("exception found, {}", e.what());
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "unexpected error, {}", err);
    } else if (!panicked) {
        return TestResult(false, "expected panic");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_Update_ManualRollback() {
    auto db = MustOpenDB();
    auto panicked = false;
    if (auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        try {
            if (auto err = tx->Rollback(); err != bolt::Success) {
                return err;
            }
        } catch (const std::exception &e) {
            panicked = true;
            fmt::println("exception found, {}", e.what());
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "unexpected error, {}", err);
    } else if (!panicked) {
        return TestResult(false, "expected panic");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_View_ManualCommit() {
    auto db = MustOpenDB();
    auto panicked = false;
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        try {
            if (auto err = tx->Commit(); err != bolt::Success) {
                return err;
            }
        } catch (const std::exception &e) {
            panicked = true;
            fmt::println("exception found, {}", e.what());
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "unexpected error, {}", err);
    } else if (!panicked) {
        return TestResult(false, "expected panic");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_View_ManualRollback() {
    auto db = MustOpenDB();
    auto panicked = false;
    if (auto err = db->View([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        try {
            if (auto err = tx->Rollback(); err != bolt::Success) {
                return err;
            }
        } catch (const std::exception &e) {
            panicked = true;
            fmt::println("exception found, {}", e.what());
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "unexpected error, {}", err);
    } else if (!panicked) {
        return TestResult(false, "expected panic");
    }
    MustCloseDB(std::move(db));
    return true; }

TestResult TestDB_Update_Panic() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        throw std::runtime_error("omg");
        return bolt::Success;
    }); err == bolt::Success) {
        return TestResult(false, "Unexpected error, {}", err);
    }

    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "Update fail, {}", err);
    }

    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (tx->Bucket(to_bytes(widgets)) == nullptr) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    }); err != bolt::Success) {
        return TestResult(false, "verify change fail, {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestDB_View_Error() { return true; }

TestResult TestDB_View_Panic() { return true; }

TestResult TestDB_Stats() { return true; }

TestResult TestDB_Consistency() { return true; }

TestResult TestDB_Stats_Sub() { return true; }

TestResult TestDB_Batch() { return true; }

TestResult TestDB_Batch_Panic() { return true; }

TestResult TestDB_BatchFull() { return true; }

TestResult TestDB_BatchTime() { return true; }
