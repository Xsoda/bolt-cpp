#ifndef __COMMON_HPP__
#define __COMMON_HPP__

#include <string>
#include <cstddef>
#include <map>
#include <span>
#include <vector>
#include <memory>
#include <chrono>
#include <limits>
#include <functional>

namespace bolt {
    using namespace std::chrono_literals;
    using pgid = std::uint64_t;
    using txid = std::uint64_t;
    using bytes = std::span<std::byte>;

    // Default values if not set in a DB instance.
    constexpr int DefaultMaxBatchSize = 100;
    constexpr std::chrono::milliseconds DefaultMaxBatchDelay = 10ms;
    constexpr int DefaultAllocSize = 16 * 1024 * 1024;

    constexpr int MaxKeySize = 32768;
    constexpr int MaxValueSize = std::numeric_limits<int>::max() - 2;
    constexpr float DefaultFillPercent = 0.5;

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
}

#endif  // __COMMON_HPP__
