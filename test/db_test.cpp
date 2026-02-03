#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/page.hpp"
#include "random.hpp"
#include "test.hpp"
#include "util.hpp"
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <iostream>
#include <cassert>

std::string tempfile() {
    auto tmpdir = std::filesystem::temp_directory_path();
    std::string filename = "bolt-";
    filename.append(RandomCharset(5));
    auto filepath = tmpdir / filename;
    if (std::filesystem::exists(filepath)) {
        std::filesystem::remove(filepath);
    }
    return filepath.string();
}

bolt::impl::DBPtr MustOpenDB() {
    auto db = std::make_shared<bolt::impl::DB>();
    auto path = tempfile();
    auto err = db->Open(path);
    if (err != bolt::ErrorCode::Success) {
        assert("open database fail" && false);
        return nullptr;
    }
    return db;
}

void MustCloseDB(bolt::impl::DBPtr &&db) {
    auto err = db->Close();
    if (err != bolt::ErrorCode::Success) {
        assert("close database fail" && false);
    }
}

void MustCheck(bolt::impl::DBPtr db) {
    auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
            auto f = tx->Check();
            auto errors = f.get();
            if (errors.size() > 0) {
                fmt::println("Database {} Check Result", db->Path());
                for (auto &item : errors) {
                    fmt::println("  - {}", item);
                }
            }
            return bolt::ErrorCode::Success;
    });
}

TestResult TestDB_Open() {
    auto db = std::make_shared<bolt::impl::DB>();
    auto path = tempfile();
    auto err = db->Open(path, false);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "expected open success");
    }
    auto s = db->Path();
    if (s != path) {
        return TestResult(false, "expected path");
    }
    err = db->Close();
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "expected close database");
    }
    return true;
}

TestResult TestDB_OpenPathRequired() {
    auto db = std::make_shared<bolt::impl::DB>();
    auto err = db->Open("", false);
    if (err == bolt::ErrorCode::Success) {
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
    if (err != bolt::ErrorCode::ErrorDatabaseInvalid) {
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
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "open database file fail");
    }
    std::tie(size, err) = file.Size();
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "stat database file fail");
    }
    buf.assign(size, std::byte(0));
    std::tie(size, err) = file.ReadAt(buf, 0);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "read database file fail");
    }
    auto meta0 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize]);
    meta0->version++;
    auto meta1 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize + bolt::impl::Getpagesize()]);
    meta1->version++;
    std::tie(size, err) = file.WriteAt(buf, 0);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "write database file fail");
    }

    db = std::make_shared<bolt::impl::DB>();
    err = db->Open(path);
    if (err != bolt::ErrorCode::ErrorVersionMismatch) {
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
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "open database file fail");
    }
    std::tie(size, err) = file.Size();
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "stat database file fail");
    }
    buf.assign(size, std::byte(0));
    std::tie(size, err) = file.ReadAt(buf, 0);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "read database file fail");
    }
    auto meta0 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize]);
    meta0->pgid++;
    auto meta1 = reinterpret_cast<bolt::impl::meta *>(
        &buf.data()[bolt::impl::pageHeaderSize + bolt::impl::Getpagesize()]);
    meta1->pgid++;
    std::tie(size, err) = file.WriteAt(buf, 0);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "write database file fail");
    }

    db = std::make_shared<bolt::impl::DB>();
    err = db->Open(path);
    if (err != bolt::ErrorCode::ErrorChecksum) {
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
      if (err != bolt::ErrorCode::Success) {
        return err;
      }
      for (int i = 0; i < 10000; i++) {
        std::string key = fmt::format("{:04d}", i);
        std::vector<std::byte> value{1000, std::byte(0)};
        err = b->Put(to_bytes(key), value);
        if (err != bolt::ErrorCode::Success) {
          return err;
        }
      }
      return bolt::ErrorCode::Success;
    });
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "database Update fail");
    }
    err = db->Close();
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "database close fail");
    }
    fmt::println("database {} closed", path);
    return true;
}
