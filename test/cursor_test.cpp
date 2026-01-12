#include "bolt/error.hpp"
#include "impl/db.hpp"
#include "impl/page.hpp"
#include "impl/cursor.hpp"
#include "random.hpp"
#include "test.hpp"
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
    return true;
}
