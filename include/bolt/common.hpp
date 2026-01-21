#pragma once

#include <type_traits>
#ifndef __COMMON_HPP__
#define __COMMON_HPP__

#ifndef BOLT_STATIC
#  ifdef _WIN32
#    ifdef BOLT_EXPORT_API
#      define BOLT_API __declspec(dllexport)
#    else
#      define BOLT_API __declspec(dllimport)
#    endif
#  else
#    if __GNUC__ >= 4
#      define BOLT_API __attribute__(visibility("default"))
#    else
#      define BOLT_API
#    endif
#  endif
#else
#  define BOLT_API
#endif

#include <string>
#include <map>
#include <span>
#include <vector>
#include <chrono>
#include <limits>
#include <functional>

namespace bolt {
    using namespace std::chrono_literals;
    using bytes = std::span<std::byte>;

    // Default values if not set in a DB instance.
    constexpr int DefaultMaxBatchSize = 100;
    constexpr std::chrono::milliseconds DefaultMaxBatchDelay = 10ms;
    constexpr int DefaultAllocSize = 16 * 1024 * 1024;

    constexpr int MaxKeySize = 32768;
    constexpr int MaxValueSize = std::numeric_limits<int>::max() - 2;
    constexpr float DefaultFillPercent = 0.5;
}

#endif  // __COMMON_HPP__
