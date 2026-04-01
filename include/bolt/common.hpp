#pragma once

#ifndef __COMMON_HPP__
#define __COMMON_HPP__

#include <chrono>
#include <limits>
#include <span>

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

} // namespace bolt

#endif // __COMMON_HPP__
