#include "bolt/bolt.hpp"
#include "fmt/format.h"
#include "fmt/std.h"
#include <cstring>
#include <filesystem>
#include <functional>
#include <iterator>
#include <set>
#include <span>
#include <string>

std::uint64_t Random() {
    static std::uint64_t seed = 13;
    seed = seed * 997 + 521;
    return seed;
}

std::string RandomCharset(size_t length) {
    std::string value;
    value.assign(length, '.');
    static const std::string charset =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for (size_t i = 0; i < length; i++) {
        std::uint64_t index = Random() % charset.size();
        value[i] = charset[index];
    }
    return std::move(value);
}

std::int64_t RandomInt(std::int64_t min, std::int64_t max) { return Random() % (max - min) + min; }

std::string RandomString(std::int64_t min, std::int64_t max) {
    static const std::string charset =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    auto size = RandomInt(min, max);
    std::string result(size, '.');
    for (int i = 0; i < size; i++) {
        std::uint64_t index = Random() % charset.size();
        result[i] = charset[index];
        // std::uint8_t ch = RandomInt(1, 255);
        // result[i] = (char)ch;
    }
    return result;
}

template <class Container> std::string to_string(const Container &container) {
    return std::string(reinterpret_cast<const char *>(container.data()), container.size());
}

enum class OP {
    Insert,
    Update,
    Delete,
};

OP GetOP() {
    auto val = Random() % 1000;
    if (val < 600) {
        return OP::Insert;
    } else if (val < 800) {
        return OP::Update;
    } else if (val < 1000) {
        return OP::Delete;
    }
    return OP::Insert;
}

int main(int argc, char **argv) {
    bolt::DB db;
    std::set<std::string> keys;
    std::filesystem::remove("chaos-cxx");
    if (auto err = db.Open("chaos-cxx"); err != bolt::ErrorCode::Success) {
        fmt::println("open {} fail, {}", db.Path(), err);
        return -1;
    }
    std::optional<long long> max_op, tx_op;
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "-max-op") == 0 && i + 1 < argc) {
            try {
                auto val = std::stoll(argv[i + 1]);
                max_op = val;
            } catch (std::exception &e) {
                fmt::println("excpetion {}", e.what());
            }
            i += 1;
        } else if (std::strcmp(argv[i], "-tx-op") == 0 && i + 1 < argc) {
            try {
                auto val = std::stoll(argv[i + 1]);
                tx_op = val;
            } catch (std::exception &e) {
                fmt::println("excpetion {}", e.what());
            }
            i += 1;
        }
    }
    auto bucket = RandomString(8, 32);
    for (int i = 0; i < max_op.value_or(100000); i += tx_op.value_or(50000)) {
        auto err = db.Update([tx_op, i, &keys, &bucket](bolt::Tx tx) -> bolt::ErrorCode {
            auto [b, err] = tx.CreateBucketIfNotExists(bolt::to_bytes(bucket));
            if (err != bolt::ErrorCode::Success) {
                return err;
            }
            for (int j = 0; j < tx_op.value_or(50000); j++) {
                auto op = GetOP();
                if (keys.size() == 0) {
                    op = OP::Insert;
                }
                if (op == OP::Insert) {
                    auto key = RandomString(8, 32);
                    auto val = RandomString(32, 4096);
                    keys.insert(key);
                    fmt::println("{:06} INSERT {}", i + j, bolt::to_bytes(key));
                    err = b.Put(bolt::to_bytes(key), bolt::to_bytes(val));
                    if (err != bolt::ErrorCode::Success) {
                        return err;
                    }
                } else if (op == OP::Update) {
                    auto idx = RandomInt(0, keys.size());
                    auto it = std::next(keys.begin(), idx);
                    auto val = RandomString(32, 4096);
                    auto key = *it;
                    err = b.Put(bolt::to_bytes(key), bolt::to_bytes(val));
                    fmt::println("{:06} UPDATE {}", i + j, bolt::to_bytes(key));
                    if (err != bolt::ErrorCode::Success) {
                        return err;
                    }
                } else if (op == OP::Delete) {
                    auto idx = RandomInt(0, keys.size());
                    auto it = std::next(keys.begin(), idx);
                    auto key = *it;
                    fmt::println("{:06} DELETE {}", i + j, bolt::to_bytes(key));
                    keys.erase(it);
                    err = b.Delete(bolt::to_bytes(key));
                    if (err != bolt::ErrorCode::Success) {
                        return err;
                    }
                }
            }
            return bolt::ErrorCode::Success;
        });
        if (err != bolt::ErrorCode::Success) {
            fmt::println("update fail, {}", err);
        }
        keys.clear();
    }
    if (auto err = db.View([&bucket](bolt::Tx tx) -> bolt::ErrorCode {
            auto b = tx.Bucket(bolt::to_bytes(bucket));
            auto val = b.Get(bolt::to_bytes("ZqReP8ryRa5y"));
            fmt::println("value: {}", val);
            return bolt::ErrorCode::Success;
        });
        err != bolt::ErrorCode::Success) {
    }
    auto stat = db.Stats().TxStats;
    fmt::println("{}", stat);
    if (auto err = db.Close(); err != bolt::ErrorCode::Success) {
        fmt::println("close {} fail, {}", db.Path(), err);
        return -1;
    }
    return 0;
}
