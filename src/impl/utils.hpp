#pragma once

#ifndef __UTILS_HPP__
#define __UTILS_HPP__

#include <cstdint>
#include <memory>
#include <chrono>

#include "bolt/common.hpp"
#include "bolt/error.hpp"

namespace bolt::impl {
    using namespace std::chrono_literals;

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
}

#endif  // !__UTILS_HPP__
