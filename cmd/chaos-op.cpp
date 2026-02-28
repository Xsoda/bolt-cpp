#include "bolt/bolt.hpp"
#include "fmt/format.h"
#include "fmt/std.h"
#include "args.hpp"
#include <string>
#include <span>
#include <set>
#include <iterator>
#include <functional>
#include <filesystem>

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

std::int64_t RandomInt(std::int64_t min, std::int64_t max) {
    return Random() % (max - min) + min;
}

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

template <typename Container>
constexpr std::span<const std::byte> to_bytes(const Container &container) {
    return std::span<const std::byte>(
        reinterpret_cast<const std::byte *>(container.data()),
        container.size());
}

template <class Container> std::string to_string(const Container &container) {
    return std::string(reinterpret_cast<const char *>(container.data()),
                       container.size());
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
    if (auto err = db.Open("chaos-cxx"); err != bolt::Success) {
        fmt::println("open {} fail, {}", db.Path(), err);
        return -1;
    }
    auto cmd = Parse(argc - 1, argv + 1);
    auto max_op = GetArgument<long long>(cmd, "max-op").value_or(10000);
    auto err = db.Update([max_op, &keys](bolt::Tx tx) -> bolt::ErrorCode {
        auto bucket = RandomString(8, 32);
        auto [b, err] = tx.CreateBucketIfNotExists(to_bytes(bucket));
        if (err != bolt::Success) {
            return err;
        }
        for (int i = 0; i < max_op; i++) {
            auto op = GetOP();
            if (op == OP::Insert) {
                auto key = RandomString(8, 32);
                auto val = RandomString(32, 4096);
                keys.insert(key);
                fmt::println("{:06} INSERT {}", i, to_bytes(key));
                err = b.Put(to_bytes(key), to_bytes(val));
                if (err != bolt::Success) {
                    return err;
                }
            } else if (op == OP::Update) {
                auto idx = RandomInt(0, keys.size());
                auto it = std::next(keys.begin(), idx);
                auto val = RandomString(32, 4096);
                auto key = *it;
                err = b.Put(to_bytes(key), to_bytes(val));
                fmt::println("{:06} UPDATE {}", i, to_bytes(key));
                if (err != bolt::Success) {
                    return err;
                }
            } else if (op == OP::Delete) {
                auto idx = RandomInt(0, keys.size());
                auto it = std::next(keys.begin(), idx);
                auto key = *it;
                fmt::println("{:06} DELETE {}", i, to_bytes(key));
                keys.erase(it);
                err = b.Delete(to_bytes(key));
                if (err != bolt::Success) {
                    return err;
                }
            }
        }
        return bolt::Success;
    });
    if (err != bolt::Success) {
        fmt::println("update fail, {}", err);
    }
    auto stat = db.Stats().TxStats;
    fmt::println("{}", stat);
    if (auto err = db.Close(); err != bolt::Success) {
        fmt::println("close {} fail, {}", db.Path(), err);
        return -1;
    }
    return 0;
}
