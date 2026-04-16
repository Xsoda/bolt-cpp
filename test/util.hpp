#pragma once
#include "bolt/error.hpp"
#include "fmt/base.h"
#include "impl/bsearch.hpp"
#include "impl/db.hpp"
#include "impl/file.hpp"
#include "impl/tx.hpp"
#include "random.hpp"
#include <algorithm>
#include <bit>
#include <cassert>
#include <filesystem>
#include <span>
#include <string>

template <class ContainerA, class ContainerB> constexpr bool Equal(ContainerA a, ContainerB b) {
    return std::is_eq(bolt::impl::compare_three_way(a, b));
}

template <typename T> constexpr std::span<const std::byte> to_bytes(const T &val) {
    if constexpr (requires {
                      val.data();
                      val.size();
                  }) {
        return std::span<const std::byte>(reinterpret_cast<const std::byte *>(val.data()),
                                          val.size());
    }
    if constexpr (std::is_convertible_v<T, const char *>) {
        return std::span<const std::byte>(reinterpret_cast<const std::byte *>(val),
                                          std::char_traits<char>::length(val));
    }
}

template <class Container> std::string to_string(const Container &container) {
    return std::string(reinterpret_cast<const char *>(container.data()), container.size());
}

template <std::integral T> constexpr T byteswap(T value) noexcept {
    union {
        T val;
        char ptr[sizeof(T)];
    } s;
    s.val = value;
    for (int i = 0; i < sizeof(T) / 2; i++) {
        auto tmp = s.ptr[i];
        s.ptr[i] = s.ptr[sizeof(T) - i - 1];
        s.ptr[sizeof(T) - i - 1] = tmp;
    }
    return s.val;
}

inline std::span<const std::byte> u64tob(std::uint64_t &v) {
    if constexpr (std::endian::native == std::endian::little) {
        v = byteswap(v);
    }
    return std::span<const std::byte>(reinterpret_cast<const std::byte *>(&v),
                                      sizeof(std::uint64_t));
}

inline std::string tempfile() {
    auto tmpdir = std::filesystem::temp_directory_path();
    std::string filename = "bolt-";
    filename.append(RandomCharset(5));
    auto filepath = tmpdir / filename;
    if (std::filesystem::exists(filepath)) {
        std::filesystem::remove(filepath);
    }
    return filepath.string();
}

inline bolt::impl::DBPtr MustOpenDB(std::string path = "") {
    auto db = std::make_shared<bolt::impl::DB>();
    if (path.empty()) {
        path = tempfile();
    }
    auto err = db->Open(path);
    if (err != bolt::Success) {
        assert("open database fail" && false);
        return nullptr;
    }
    return db;
}

inline void MustCloseDB(bolt::impl::DBPtr &&db) {
    auto err = db->Close();
    if (err != bolt::Success) {
        assert("close database fail" && false);
    }
}

inline void MustCheck(bolt::impl::DBPtr db) {
    auto err = db->Update([&](bolt::impl::TxPtr tx) -> bolt::ErrorCode {
        auto f = tx->Check();
        auto errors = f.get();
        if (errors.size() > 0) {
            fmt::println("Database {} Check Result", db->Path());
            for (auto &item : errors) {
                fmt::println("  - {}", item);
            }
        }
        return bolt::Success;
    });
}
