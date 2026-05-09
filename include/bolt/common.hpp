#pragma once

#ifndef __COMMON_HPP__
#define __COMMON_HPP__

#include <chrono>
#include <limits>
#include <span>
#include <string>

#define BOLT_MAJOR_VERSION 0
#define BOLT_MINOR_VERSION 1
#define BOLT_PATCH_VERSION 2

namespace bolt {

using namespace std::chrono_literals;
using bytes = std::span<std::byte>;
using const_bytes = std::span<const std::byte>;

// Default values if not set in a DB instance.
constexpr int DefaultMaxBatchSize = 100;
constexpr std::chrono::milliseconds DefaultMaxBatchDelay = 10ms;
constexpr int DefaultAllocSize = 16 * 1024 * 1024;

constexpr int MaxKeySize = 32768;
constexpr int MaxValueSize = std::numeric_limits<int>::max() - 2;
constexpr float DefaultFillPercent = 0.5;

template <typename...> inline constexpr bool always_false = false;

template <typename T> constexpr std::span<const std::byte> to_bytes(const T &val) {
    if constexpr (requires {
                      val.data();
                      val.size();
                  }) {
        return std::span<const std::byte>(reinterpret_cast<const std::byte *>(val.data()),
                                          val.size());
    } else if constexpr (std::is_convertible_v<T, const char *>) {
        return std::span<const std::byte>(reinterpret_cast<const std::byte *>(val),
                                          std::char_traits<char>::length(val));
    } else {
        static_assert(always_false<T>, "Unsupport convert to bytes");
    }
}

constexpr std::span<const std::byte> to_bytes(const void *ptr, size_t byte_length) {
    return {static_cast<const std::byte *>(ptr), byte_length};
}

constexpr std::span<const std::byte> to_bytes(void *ptr, size_t byte_length) {
    return to_bytes(static_cast<const void *>(ptr), byte_length);
}

} // namespace bolt

#endif // __COMMON_HPP__
