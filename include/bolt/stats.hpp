#pragma once
#include "fmt/base.h"
#ifndef BOLT_STATS_HPP
#define BOLT_STATS_HPP

#include "common.hpp"
#include "fmt/format.h"
#include "fmt/std.h"

namespace bolt {

struct TxStats {
    // Page statistics.
    size_t PageCount;
    size_t PageAlloc;

    // Cursor statistics.
    size_t CursorCount;

    // Node statistics
    size_t NodeCount;
    size_t NodeDeref;

    // Rebalance statistics.
    size_t Rebalance;
    std::chrono::milliseconds RebalanceTime;

    // Split/Spill statistics.
    size_t Split;
    size_t Spill;
    std::chrono::milliseconds SpillTime;

    // Write statistics.
    size_t Write;
    std::chrono::milliseconds WriteTime;

    TxStats()
        : PageCount(0), PageAlloc(0), CursorCount(0), NodeCount(0), NodeDeref(0),
          Rebalance(0), RebalanceTime(0ms), Split(0), Spill(0), SpillTime(0ms),
          Write(0), WriteTime(0ms){};

    ~TxStats() = default;
    TxStats(const TxStats &other) {
        PageCount = other.PageCount;
        PageAlloc = other.PageAlloc;
        CursorCount = other.CursorCount;
        NodeCount = other.NodeCount;
        NodeDeref = other.NodeDeref;
        Rebalance = other.Rebalance;
        RebalanceTime = other.RebalanceTime;
        Split = other.Split;
        Spill = other.Spill;
        SpillTime = other.SpillTime;
        Write = other.Write;
        WriteTime = other.WriteTime;
    };
    TxStats &operator+=(const TxStats &other) {
        PageCount += other.PageCount;
        PageAlloc += other.PageAlloc;
        CursorCount += other.CursorCount;
        NodeCount += other.NodeCount;
        NodeDeref += other.NodeDeref;
        Rebalance += other.Rebalance;
        RebalanceTime += other.RebalanceTime;
        Split += other.Split;
        Spill += other.Spill;
        SpillTime += other.SpillTime;
        Write += other.Write;
        WriteTime += other.WriteTime;
        return *this;
    };
    friend TxStats operator+(TxStats lhs, const TxStats &rhs) {
        TxStats result;
        result.PageCount += lhs.PageCount + rhs.PageCount;
        result.PageAlloc += lhs.PageAlloc + rhs.PageAlloc;
        result.CursorCount += lhs.CursorCount + rhs.CursorCount;
        result.NodeCount += lhs.NodeCount + rhs.NodeCount;
        result.NodeDeref += lhs.NodeDeref + rhs.NodeDeref;
        result.Rebalance += lhs.Rebalance + rhs.Rebalance;
        result.RebalanceTime += lhs.RebalanceTime + rhs.RebalanceTime;
        result.Split += lhs.Split + rhs.Split;
        result.Spill += lhs.Spill + rhs.Spill;
        result.SpillTime += lhs.SpillTime + rhs.SpillTime;
        result.Write += lhs.Write + rhs.Write;
        result.WriteTime += lhs.WriteTime + rhs.WriteTime;
        return result;
    };
    friend TxStats operator-(TxStats lhs, const TxStats &rhs) {
        TxStats result;
        result.PageCount += lhs.PageCount - rhs.PageCount;
        result.PageAlloc += lhs.PageAlloc - rhs.PageAlloc;
        result.CursorCount += lhs.CursorCount - rhs.CursorCount;
        result.NodeCount += lhs.NodeCount - rhs.NodeCount;
        result.NodeDeref += lhs.NodeDeref - rhs.NodeDeref;
        result.Rebalance += lhs.Rebalance - rhs.Rebalance;
        result.RebalanceTime += lhs.RebalanceTime - rhs.RebalanceTime;
        result.Split += lhs.Split - rhs.Split;
        result.Spill += lhs.Spill - rhs.Spill;
        result.SpillTime += lhs.SpillTime - rhs.SpillTime;
        result.Write += lhs.Write - rhs.Write;
        result.WriteTime += lhs.WriteTime - rhs.WriteTime;
        return result;
    };
    TxStats &operator=(const TxStats &other) {
        if (this == &other) {
            return *this;
        }
        PageCount = other.PageCount;
        PageAlloc = other.PageAlloc;
        CursorCount = other.CursorCount;
        NodeCount = other.NodeCount;
        NodeDeref = other.NodeDeref;
        Rebalance = other.Rebalance;
        RebalanceTime = other.RebalanceTime;
        Split = other.Split;
        Spill = other.Spill;
        SpillTime = other.SpillTime;
        Write = other.Write;
        WriteTime = other.WriteTime;
        return *this;
    };
};

struct Stats {
  size_t FreePageN;
  size_t PendingPageN;
  size_t FreeAlloc;
  size_t FreelistInuse;
  size_t TxN;
  size_t OpenTxN;
  bolt::TxStats TxStats;
  Stats()
      : FreePageN(0), PendingPageN(0), FreeAlloc(0), FreelistInuse(0), TxN(0),
        OpenTxN(0){};
  ~Stats() = default;
  friend Stats operator-(Stats lhs, const Stats rhs) {
    Stats result;
    result.FreePageN = lhs.FreePageN;
    result.PendingPageN = lhs.PendingPageN;
    result.FreeAlloc = lhs.FreeAlloc;
    result.TxN = lhs.TxN - rhs.TxN;
    result.TxStats = lhs.TxStats - rhs.TxStats;
    return result;
  };
  friend Stats operator+(Stats lhs, const Stats rhs) {
    Stats result;
    result.TxStats = lhs.TxStats + rhs.TxStats;
    return result;
  };
};

struct BucketStats {
    int BranchPageN;
    int BranchOverflowN;
    int LeafPageN;
    int LeafOverflowN;
    int KeyN;
    int Depth;
    int BranchAlloc;
    int BranchInuse;
    int LeafAlloc;
    int LeafInuse;
    int BucketN;
    int InlineBucketN;
    int InlineBucketInuse;
    BucketStats()
        : BranchPageN(0), BranchOverflowN(0), LeafPageN(0), LeafOverflowN(0),
          KeyN(0), Depth(0), BranchAlloc(0), BranchInuse(0), LeafAlloc(0),
          LeafInuse(0), BucketN(0), InlineBucketN(0), InlineBucketInuse(0){};
    ~BucketStats() = default;
    BucketStats(const BucketStats &other) {
        BranchPageN = other.BranchPageN;
        BranchOverflowN = other.BranchOverflowN;
        LeafPageN = other.LeafPageN;
        LeafOverflowN = other.LeafOverflowN;
        KeyN = other.KeyN;
        Depth = other.Depth;
        BranchAlloc = other.BranchAlloc;
        BranchInuse = other.BranchInuse;
        LeafAlloc = other.LeafAlloc;
        LeafInuse = other.LeafInuse;
        BucketN = other.BucketN;
        InlineBucketN = other.InlineBucketN;
        InlineBucketInuse = other.InlineBucketInuse;
    };
    BucketStats &operator+=(const BucketStats &other) {
        BranchPageN += other.BranchPageN;
        BranchOverflowN += other.BranchOverflowN;
        LeafPageN += other.LeafPageN;
        LeafOverflowN += other.LeafOverflowN;
        KeyN += other.KeyN;
        if (Depth < other.Depth) {
            Depth = other.Depth;
        }
        BranchAlloc += other.BranchAlloc;
        BranchInuse += other.BranchInuse;
        LeafAlloc += other.LeafAlloc;
        LeafInuse += other.LeafInuse;
        BucketN += other.BucketN;
        InlineBucketN += other.InlineBucketN;
        InlineBucketInuse += other.InlineBucketInuse;
        return *this;
    };
    BucketStats &operator=(const BucketStats &other) {
        if (this == &other) {
            return *this;
        }
        BranchPageN = other.BranchPageN;
        BranchOverflowN = other.BranchOverflowN;
        LeafPageN = other.LeafPageN;
        LeafOverflowN = other.LeafOverflowN;
        KeyN = other.KeyN;
        Depth = other.Depth;
        BranchAlloc = other.BranchAlloc;
        BranchInuse = other.BranchInuse;
        LeafAlloc = other.LeafAlloc;
        LeafInuse = other.LeafInuse;
        BucketN = other.BucketN;
        InlineBucketN = other.InlineBucketN;
        InlineBucketInuse = other.InlineBucketInuse;
        return *this;
    };
};

struct Info {
  std::uint32_t PageSize;
  std::uintptr_t Data;
};

} // namespace bolt

FMT_BEGIN_NAMESPACE

template <>
struct formatter<bolt::TxStats> : nested_formatter<fmt::string_view> {
    auto format(bolt::TxStats &stats, format_context &ctx) const
        -> decltype(ctx.out()) {
        return write_padded(ctx, [this, &stats](auto out) -> decltype(out) {
            out = fmt::format_to(out, "PageCount: {}, PageAlloc: {}\n",
                                 stats.PageCount, stats.PageAlloc);
            out = fmt::format_to(out, "CursorCount: {}\n", stats.CursorCount);
            out = fmt::format_to(out, "NodeCount: {}, NodeDeref: {}\n",
                                 stats.NodeCount, stats.NodeDeref);
            out = fmt::format_to(out, "Rebalance: {}, RebalanceTime: {}\n",
                                 stats.Rebalance, stats.RebalanceTime);
            out = fmt::format_to(out, "Split: {}, Spill: {}, SpillTime: {}\n",
                                 stats.Split, stats.Spill, stats.SpillTime);
            out = fmt::format_to(out, "Write: {}, WriteTime: {}", stats.Write,
                                 stats.WriteTime);
            return out;
        });
    };
};

template <>
struct formatter<bolt::Stats> : nested_formatter<fmt::string_view> {
    auto format(bolt::Stats &stats, format_context &ctx) const
        -> decltype(ctx.out()) {
        return write_padded(ctx, [this, &stats](auto out) -> decltype(out) {
            out = fmt::format_to(out, "FreePageN: {}, PendingPageN: {}\n",
                                 stats.FreePageN, stats.PendingPageN);
            out = fmt::format_to(out, "FreeAlloc: {}, FreelistInuse: {}\n",
                                 stats.FreeAlloc, stats.FreelistInuse);
            out = fmt::format_to(out, "TxN: {}, OpenTxN: {}\n", stats.TxN,
                                 stats.OpenTxN);
            out = fmt::format_to(out, "{}", stats.TxStats);
            return out;
        });
    };
};

template <>
struct formatter<bolt::BucketStats> : nested_formatter<fmt::string_view> {
    auto format(bolt::BucketStats &stats, format_context &ctx) const
        -> decltype(ctx.out()) {
        return write_padded(ctx, [this, &stats](auto out) -> decltype(out) {
            out = fmt::format_to(out, "BranchPageN: {}, BranchOverflowN: {}\n",
                               stats.BranchPageN, stats.BranchOverflowN);
            out = fmt::format_to(out, "LeafPageN: {}, LeafOverflowN: {}\n",
                                 stats.LeafPageN, stats.LeafOverflowN);
            out = fmt::format_to(out, "KeyN: {}\n", stats.KeyN);
            out = fmt::format_to(out, "Depth: {}\n", stats.Depth);
            out = fmt::format_to(out, "BranchAlloc: {}, BranchInuse: {}\n",
                                 stats.BranchAlloc, stats.BranchInuse);
            out = fmt::format_to(out, "LeafAlloc: {}, LeafInuse: {}\n",
                                 stats.LeafAlloc, stats.LeafInuse);
            out = fmt::format_to(out, "BucketN: {}\n", stats.BucketN);
            out =
                fmt::format_to(out, "InlineBucketN: {}, InlineBucketInuse: {}",
                               stats.InlineBucketN, stats.InlineBucketInuse);
            return out;
        });
    }
};

FMT_END_NAMESPACE
#endif  // !BOLT_STATS_HPP
