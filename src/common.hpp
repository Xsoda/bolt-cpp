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

namespace bolt {
    using namespace std::chrono_literals;
    using pgid = std::uint64_t;
    using txid = std::uint64_t;
    using bytes = std::span<std::byte>;

    const int DefaultMaxBatchSize = 100;
    const std::chrono::milliseconds DefaultMaxBatchDelay = 10ms;
    const int DefaultAllocSize = 16 * 1024 * 1024;
    const int MaxKeySize = 32768;
    const int MaxValueSize = std::numeric_limits<int>::max() - 2;
    const float DefaultFillPercent = 0.5;

    const std::uint32_t magic = 0xED0CDAED;
    const std::uint32_t version = 2;
    const std::uint32_t maxMmapStep = 1 << 30;


    const int defaultPageSize = 4 * 1024;
    const int branchPageFlag = 0x01;
    const int leafPageFlag = 0x02;
    const int metaPageFlag = 0x04;
    const int freelistPageFlag = 0x10;
    const int bucketLeafFlag = 0x01;
    const int minKeysPerPage = 2;

}

#endif  // __COMMON_HPP__
