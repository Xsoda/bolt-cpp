#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "random.hpp"
#include "test.hpp"
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

void MustClose(bolt::impl::DBPtr db) {
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
        return TestResult(false, "expected open error code invalid");
    }
    return true;
}
