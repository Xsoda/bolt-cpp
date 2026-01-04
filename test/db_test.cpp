#include "db.hpp"
#include "test.hpp"
#include <filesystem>
#include <memory>
#include <fstream>

std::string tempfile() {
	auto tmpdir = std::filesystem::temp_directory_path();
        auto filepath = tmpdir;
        filepath /= "bolt-";
	if (std::filesystem::exists(filepath)) {
		std::filesystem::remove(filepath);
	}
	return filepath.string();
}

TestResult TestDB_Open() {
    auto db = std::make_shared<bolt::DB>();
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
    auto db = std::make_shared<bolt::DB>();
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

    auto db = std::make_shared<bolt::DB>();
    auto err = db->Open(path, true);
    if (err != bolt::ErrorCode::ErrorDatabaseInvalid) {
        return TestResult(false, "expected open error code invalid");
    }
    return true;
}
