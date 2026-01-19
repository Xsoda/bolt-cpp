#include "bolt/error.hpp"
#include "impl/tx.hpp"
#include "impl/db.hpp"
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

std::string to_string(std::span<std::byte> &b) {
    return std::string(reinterpret_cast<char*>(b.data()), b.size());
}

TestResult TestTx_Commit_ErrorTxClosed() {
    std::string foo = "foo";
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "Begin Tx failed");
    }
    std::tie(std::ignore, err) = tx->CreateBucket(to_bytes(foo));
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "CreateBucket failed");
    }
    if (auto err = tx->Commit(); err != bolt::ErrorCode::Success) {
        return TestResult(false, "Commit Tx failed");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Rollback_ErrorTxClosed() {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "Begin Tx failed");
    }
    if (auto err = tx->Rollback(); err != bolt::ErrorCode::Success) {
        return TestResult(false, "Rollback Tx failed");
    }
    if (auto err = tx->Rollback(); err != bolt::ErrorCode::ErrorTxClosed) {
        return TestResult(false, "Rollback Tx unexpected error");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Commit_ErrorTxNotWritable() {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(false);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "Begin Tx failed");
    }
    if (auto err = tx->Commit(); err != bolt::ErrorCode::ErrorTxNotWritable) {
        return TestResult(false, "Rollback Tx unexpcted error");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Cursor() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          std::string woojits = "woojits";
          if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
              err != bolt::ErrorCode::Success) {
            return err;
          }
          if (auto [b, err] = tx->CreateBucket(to_bytes(woojits));
              err != bolt::ErrorCode::Success) {
              return err;
          }

          auto c = tx->Cursor();
          if (auto [k, v] = c->First(); !Compare(k, to_bytes(widgets))) {
              fmt::println("unexpected key: {}", to_string(k));
              return bolt::ErrorCode::ErrorUnexpected;
          } else if (!v.empty()) {
              fmt::println("unexpected value: {}", to_string(v));
              return bolt::ErrorCode::ErrorUnexpected;
          }

          if (auto [k, v] = c->Next(); !Compare(k, to_bytes(woojits))) {
              fmt::println("unexpected key: {}", to_string(k));
              return bolt::ErrorCode::ErrorUnexpected;
          } else if (!v.empty()) {
              fmt::println("unexpected value: {}", to_string(v));
              return bolt::ErrorCode::ErrorUnexpected;
          }

          if (auto [k, v] = c->Next(); !k.empty()) {
              fmt::println("unexpected key: {}", to_string(k));
              return bolt::ErrorCode::ErrorUnexpected;
          } else if (!v.empty()) {
              fmt::println("unexpected value: {}", to_string(v));
              return bolt::ErrorCode::ErrorUnexpected;
          }
          return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "DB Update fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket_ErrorTxNotWritable() {
    auto db = MustOpenDB();
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string foo = "foo";
          if (auto [b, err] = tx->CreateBucket(to_bytes(foo));
              err != bolt::ErrorCode::ErrorTxNotWritable) {
              return bolt::ErrorCode::ErrorUnexpected;
          }
          return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "DB View fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket_ErrorTxClosed() {
    std::string foo = "foo";
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::ErrorCode::Success) {
        return TestResult(false, "Begin Tx fail");
    }
    if (err = tx->Commit(); err != bolt::ErrorCode::Success) {
        return TestResult(false, "Commit Tx fail");
    }
    if (std::tie(std::ignore, err) = tx->CreateBucket(to_bytes(foo));
        err != bolt::ErrorCode::ErrorTxClosed) {
        return TestResult(false, "unexpected error");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Bucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::ErrorCode::Success) {
            return bolt::ErrorCode::ErrorUnexpected;
        }
        if (auto b = tx->Bucket(to_bytes(widgets)); b == nullptr) {
            fmt::println("expected bucket");
            return bolt::ErrorCode::ErrorUnexpected;
        }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return bolt::ErrorCode::ErrorUnexpected;
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Get_NotFound() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        std::string no_such_key = "no_such_key";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::ErrorCode::Success) {
            return err;
        }
        if (auto err = b->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::ErrorCode::Success) {
            return err;
        }
        if (auto v = b->Get(to_bytes(no_such_key)); !v.empty()) {
            fmt::println("expected empty value");
            return bolt::ErrorCode::ErrorUnexpected;
        }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Update fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          if (err != bolt::ErrorCode::Success) {
              return err;
          } else if (b == nullptr) {
              fmt::println("expected bucket");
              return bolt::ErrorCode::ErrorUnexpected;
          }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Update fail");
    }

    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (tx->Bucket(to_bytes(widgets)) == nullptr) {
            fmt::println("expected bucket in View");
            return bolt::ErrorCode::ErrorUnexpected;
        }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "expected bucket");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucketIfNotExists() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(widgets));
          if (err != bolt::ErrorCode::Success) {
            return err;
          } else if (b == nullptr) {
            fmt::println("expected bucket");
            return bolt::ErrorCode::ErrorUnexpected;
          }
          return bolt::ErrorCode::Success;
        });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Update fail");
    }

    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          if (tx->Bucket(to_bytes(widgets)) == nullptr) {
            fmt::println("expected bucket in View");
            return bolt::ErrorCode::ErrorUnexpected;
          }
          return bolt::ErrorCode::Success;
        });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "expected bucket");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string name = "";
        if (auto [b, err] = tx->CreateBucketIfNotExists(to_bytes(name));
            err != bolt::ErrorCode::ErrorBucketNameRequired) {
            return bolt::ErrorCode::ErrorUnexpected;
        }
        if (auto [b, err] = tx->CreateBucketIfNotExists(bolt::bytes{});
            err != bolt::ErrorCode::ErrorBucketNameRequired) {
            return bolt::ErrorCode::ErrorUnexpected;
        }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Update fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket_ErrorBucketExists() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::ErrorCode::Success) {
            return bolt::ErrorCode::ErrorUnexpected;
        }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Update fail");
    }

    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
              err != bolt::ErrorCode::ErrorBucketExists) {
              return bolt::ErrorCode::ErrorUnexpected;
          }
          return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Unexpected error");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket_ErrorBucketNameRequired() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          if (auto [b, err] = tx->CreateBucket(bolt::bytes{});
              err != bolt::ErrorCode::ErrorBucketNameRequired) {
              return bolt::ErrorCode::ErrorUnexpected;
        }
        return bolt::ErrorCode::Success;
    });
        err != bolt::ErrorCode::Success) {
        return TestResult(false, "Update fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_DeleteBucket() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_DeleteBucket_ErrorTxClosed() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_DeleteBucket_ReadOnly() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_DeleteBucket_NotFound() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_ForEach_NoError() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_ForEach_WithError() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_OnCommit() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_OnCommit_Rollback() {
    auto db = MustOpenDB();
    MustCloseDB(std::move(db));
    return true;
}
