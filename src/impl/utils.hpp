#pragma once

#ifndef __UTILS_HPP__
#define __UTILS_HPP__

#include "bolt/common.hpp"
#include "bolt/error.hpp"
#include "bolt/stats.hpp"
#include "fmt/core.h"
#include <memory>
#include <source_location>
#include <stdexcept>

namespace bolt::impl {

// Represents a marker value to indicate that a file is a Bolt DB.
constexpr std::uint32_t magic = 0xED0CDAED;

// The data file format version.
constexpr std::uint32_t version = 2;
constexpr std::uint32_t maxAllocSize = 0x7FFFFFFF;
constexpr std::uint64_t maxMapSize = 0xFFFFFFFFFFFF; // 256TB

constexpr int defaultPageSize = 4 * 1024;
constexpr int branchPageFlag = 0x01;
constexpr int leafPageFlag = 0x02;
constexpr int metaPageFlag = 0x04;
constexpr int freeListPageFlag = 0x10;
constexpr int bucketLeafFlag = 0x01;
constexpr int minKeysPerPage = 2;

struct DB;
struct Tx;
struct Cursor;
struct Bucket;
struct node;
using DBPtr = std::shared_ptr<DB>;
using TxPtr = std::shared_ptr<Tx>;
using BucketPtr = std::shared_ptr<Bucket>;
using node_ptr = std::shared_ptr<node>;
using CursorPtr = std::shared_ptr<Cursor>;

using pgid = std::uint64_t;
using txid = std::uint64_t;

template <typename... Args>
void __assert(bool condition, const std::source_location &location, fmt::format_string<Args...> fmt,
              Args &&...args) {
    if (!condition) {
        fmt::println(stderr, "Assert Failed at {}:{} {}", location.file_name(), location.line(),
                     location.function_name());
        throw std::runtime_error(fmt::format(fmt, std::forward<Args>(args)...));
    }
};

template <typename... Args>
void __log(const std::source_location &location, fmt::format_string<Args...> fmt, Args &&...args) {
    fmt::println(fmt, std::forward<Args>(args)...);
}

inline std::vector<std::string> string_split(const std::string &str, const std::string &delimiter) {
    std::vector<std::string> result;
    std::size_t start = 0;
    while (true) {
        std::size_t pos = str.find(delimiter, start);
        if (pos == std::string::npos) {
            result.push_back(str.substr(start));
            break;
        }
        result.push_back(str.substr(start, pos - start));
        start = pos + delimiter.size();
    }
    return result;
}

inline std::string string_join(const std::vector<std::string> &source,
                               const std::string &delimiter) {
    std::string result;
    for (auto i = 0; i < source.size(); i++) {
        if (i > 0) {
            result += delimiter;
        }
        result += source[i];
    }
    return result;
}

} // namespace bolt::impl

#define _assert(condition, format, ...)                                                            \
    bolt::impl::__assert(condition, std::source_location::current(), format, ##__VA_ARGS__)
#ifndef NDEBUG
#define log_debug(format, ...)                                                                     \
    bolt::impl::__log(std::source_location::current(), format, ##__VA_ARGS__)
#else
#define log_debug(format, ...)
#endif

#endif // !__UTILS_HPP__
