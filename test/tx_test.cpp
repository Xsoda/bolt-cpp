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


extern bolt::impl::DBPtr MustOpenDB();
extern void MustCloseDB(bolt::impl::DBPtr &&db);

TestResult TestTx_Commit_ErrorTxClosed() {
    std::string foo = "foo";
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx failed");
    }
    std::tie(std::ignore, err) = tx->CreateBucket(to_bytes(foo));
    if (err != bolt::Success) {
        return TestResult(false, "CreateBucket failed");
    }
    if (auto err = tx->Commit(); err != bolt::Success) {
        return TestResult(false, "Commit Tx failed");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Rollback_ErrorTxClosed() {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx failed: {}", err);
    }
    if (auto err = tx->Rollback(); err != bolt::Success) {
        return TestResult(false, "Rollback Tx failed: {}", err);
    }
    if (auto err = tx->Rollback(); err != bolt::ErrorTxClosed) {
        return TestResult(false, "Rollback Tx unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Commit_ErrorTxNotWritable() {
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(false);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx failed: {}", err);
    }
    if (auto err = tx->Commit(); err != bolt::ErrorTxNotWritable) {
        return TestResult(false, "Rollback Tx unexpcted error: {}", err);
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
              err != bolt::Success) {
            return err;
          }
          if (auto [b, err] = tx->CreateBucket(to_bytes(woojits));
              err != bolt::Success) {
              return err;
          }

          auto c = tx->Cursor();
          if (auto [k, v] = c->First(); !Equal(k, to_bytes(widgets))) {
              fmt::println("unexpected key: {}", k);
              return bolt::ErrorUnexpected;
          } else if (!v.empty()) {
              fmt::println("unexpected value: {}", v);
              return bolt::ErrorUnexpected;
          }

          if (auto [k, v] = c->Next(); !Equal(k, to_bytes(woojits))) {
              fmt::println("unexpected key: {}", k);
              return bolt::ErrorUnexpected;
          } else if (!v.empty()) {
              fmt::println("unexpected value: {}", v);
              return bolt::ErrorUnexpected;
          }

          if (auto [k, v] = c->Next(); !k.empty()) {
              fmt::println("unexpected key: {}", k);
              return bolt::ErrorUnexpected;
          } else if (!v.empty()) {
              fmt::println("unexpected value: {}", v);
              return bolt::ErrorUnexpected;
          }
          return bolt::Success;
    });
        err != bolt::Success) {
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
              err != bolt::ErrorTxNotWritable) {
              return bolt::ErrorUnexpected;
          }
          return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "DB View fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket_ErrorTxClosed() {
    std::string foo = "foo";
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx fail: {}", err);
    }
    if (err = tx->Commit(); err != bolt::Success) {
        return TestResult(false, "Commit Tx fail: {}", err);
    }
    if (std::tie(std::ignore, err) = tx->CreateBucket(to_bytes(foo));
        err != bolt::ErrorTxClosed) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_Bucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return bolt::ErrorUnexpected;
        }
        if (auto b = tx->Bucket(to_bytes(widgets)); b == nullptr) {
            fmt::println("expected bucket");
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "update fail: {}", err);
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
        if (err != bolt::Success) {
            return err;
        }
        if (auto err = b->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        if (auto v = b->Get(to_bytes(no_such_key)); !v.empty()) {
            fmt::println("expected empty value");
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
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
          if (err != bolt::Success) {
              return err;
          } else if (b == nullptr) {
              fmt::println("expected bucket");
              return bolt::ErrorUnexpected;
          }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail");
    }

    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (tx->Bucket(to_bytes(widgets)) == nullptr) {
            fmt::println("expected bucket in View");
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
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
          if (err != bolt::Success) {
            return err;
          } else if (b == nullptr) {
            fmt::println("expected bucket");
            return bolt::ErrorUnexpected;
          }
          return bolt::Success;
        });
        err != bolt::Success) {
        return TestResult(false, "Update fail");
    }

    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          if (tx->Bucket(to_bytes(widgets)) == nullptr) {
            fmt::println("expected bucket in View");
            return bolt::ErrorUnexpected;
          }
          return bolt::Success;
        });
        err != bolt::Success) {
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
            err != bolt::ErrorBucketNameRequired) {
            return bolt::ErrorUnexpected;
        }
        if (auto [b, err] = tx->CreateBucketIfNotExists(bolt::bytes{});
            err != bolt::ErrorBucketNameRequired) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
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
            err != bolt::Success) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail");
    }

    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
              err != bolt::ErrorBucketExists) {
              return bolt::ErrorUnexpected;
          }
          return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Unexpected error");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_CreateBucket_ErrorBucketNameRequired() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          if (auto [b, err] = tx->CreateBucket(bolt::bytes{});
              err != bolt::ErrorBucketNameRequired) {
              return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail");
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_DeleteBucket() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        std::string bar = "bar";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (auto err = b->Put(to_bytes(foo), to_bytes(bar));
            err != bolt::Success) {
            return err;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Update fail");
    }
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        if (auto err = tx->DeleteBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        if (auto b = tx->Bucket(to_bytes(widgets));
            b != nullptr) {
            return bolt::ErrorUnexpected;
        }
        return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "Unexpected bucket");
    }
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        std::string foo = "foo";
        auto [b, err] = tx->CreateBucket(to_bytes(widgets));
        if (err != bolt::Success) {
            return err;
        }
        if (auto v = b->Get(to_bytes(foo)); !v.empty()) {
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

TestResult TestTx_DeleteBucket_ErrorTxClosed() {
    std::string foo = "foo";
    auto db = MustOpenDB();
    auto [tx, err] = db->Begin(true);
    if (err != bolt::Success) {
        return TestResult(false, "Begin Tx fail");
    }
    if (auto err = tx->Commit(); err != bolt::Success) {
        return TestResult(false, "Commit Tx fail");
    }
    if (auto err = tx->DeleteBucket(to_bytes(foo));
        err != bolt::ErrorTxClosed) {
        return TestResult(false, "Unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_DeleteBucket_ReadOnly() {
    auto db = MustOpenDB();
    if (auto err = db->View([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string foo = "foo";
        if (auto err = tx->DeleteBucket(to_bytes(foo));
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

TestResult TestTx_DeleteBucket_NotFound() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          if (auto err = tx->DeleteBucket(to_bytes(widgets));
              err != bolt::ErrorBucketNotFound) {
              return err;
          }
          return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_ForEach_NoError() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          std::string foo = "foo";
          std::string bar = "bar";
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          if (err != bolt::Success) {
              return err;
          }
          if (auto err = b->Put(to_bytes(foo), to_bytes(bar));
              err != bolt::Success) {
              return err;
          }
          if (auto err =
              tx->ForEach([](bolt::const_bytes name,
                             bolt::impl::BucketPtr b) -> bolt::ErrorCode {
                  return bolt::Success;
              });
              err != bolt::Success) {
              return err;
          }
          return bolt::Success;
    });
        err != bolt::Success) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_ForEach_WithError() {
    auto db = MustOpenDB();
    if (auto err = db->Update([](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          std::string foo = "foo";
          std::string bar = "bar";
          auto [b, err] = tx->CreateBucket(to_bytes(widgets));
          if (err != bolt::Success) {
            return err;
          }
          if (auto err = b->Put(to_bytes(foo), to_bytes(bar));
              err != bolt::Success) {
            return err;
          }
          if (auto err =
                  tx->ForEach([](bolt::const_bytes name,
                                 bolt::impl::BucketPtr b) -> bolt::ErrorCode {
                      return bolt::ErrorUnexpected;
                  });
              err != bolt::Success) {
              return err;
          }
          return bolt::Success;
        });
        err != bolt::ErrorUnexpected) {
        return TestResult(false, "unexpected error: {}", err);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_OnCommit() {
    auto db = MustOpenDB();
    int x = 0;
    if (auto err = db->Update([&x](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
          std::string widgets = "widgets";
          tx->OnCommit([&x]() { x += 1; });
          tx->OnCommit([&x]() { x += 2; });
          if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
              err != bolt::Success) {
            return err;
          }
          return bolt::Success;
        });
        err != bolt::Success) {
        return TestResult(false, "unexpected error: {}", err);
    } else if (x != 3) {
        return TestResult(false, "unexpecte x: {}", x);
    }
    MustCloseDB(std::move(db));
    return true;
}

TestResult TestTx_OnCommit_Rollback() {
    auto db = MustOpenDB();
    int x = 0;
    if (auto err = db->Update([&x](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        std::string widgets = "widgets";
        tx->OnCommit([&x]() { x += 1; });
        tx->OnCommit([&x]() { x += 2; });
        if (auto [b, err] = tx->CreateBucket(to_bytes(widgets));
            err != bolt::Success) {
            return err;
        }
        return bolt::ErrorExpected; // rollback this commit
    });
        err != bolt::ErrorExpected) {
        return TestResult(false, "unexpected error: {}", err);
    } else if (x != 0) {
        return TestResult(false, "unexpecte x: {}", x);
    }
    MustCloseDB(std::move(db));
    return true;
}
