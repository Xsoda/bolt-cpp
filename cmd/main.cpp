#include "args.hpp"
#include "bolt/bolt.hpp"
#include "impl/file.hpp"
#include "impl/meta.hpp"
#include "impl/page.hpp"
#include <locale>
#include <span>

std::string hexdump(std::uint64_t offset, const std::span<const std::byte> bytes) {
    std::string result;
    auto size = 0;
    result += "┌────────┬─────────────────────────┬─────────────────────────┬────────"
              "┬────────┐\n";
    for (size = 0; size + 16 < bytes.size(); size += 16) {
        result += fmt::format("│{:08x}│", offset + size);
        for (int i = 0; i < 16; i++) {
            if (i == 8) {
                result += " ┊";
            }
            result += fmt::format(" {:02x}", bytes[size + i]);
        }
        result += " │";
        for (int i = 0; i < 16; i++) {
            if (i == 8) {
                result += "┊";
            }
            if (std::isprint((char)bytes[size + i], std::locale::classic())) {
                result += fmt::format("{}", (char)bytes[size + i]);
            } else {
                if (bytes[size + i] == std::byte(0)) {
                    result += "⋄";
                } else {
                    result += "×";
                }
            }
        }
        result += fmt::format("{}\n", "│");
    }
    if (size < bytes.size()) {
        result += fmt::format("│{:08x}│", offset + size);
        for (int i = 0; i < 16; i++) {
            if (i == 8) {
                result += " ┊";
            }
            if (size + i < bytes.size()) {
                result += fmt::format(" {:02x}", bytes[size + i]);
            } else {
                result += fmt::format(" {}", "  ");
            }
        }
        result += fmt::format("{}", " │");
        for (int i = 0; i < 16; i++) {
            if (i == 8) {
                result += "┊";
            }
            if (size + i < bytes.size()) {
                if (std::isprint((char)bytes[size + i], std::locale::classic())) {
                    result += fmt::format("{}", (char)bytes[size + i]);
                } else {
                    if (bytes[size + i] == std::byte(0)) {
                        result += "⋄";
                    } else {
                        result += "×";
                    }
                }
            } else {
                result += " ";
            }
        }
        result += fmt::format("{}\n", "│");
    }
    result += "└────────┴─────────────────────────┴─────────────────────────┴──"
              "──────┴────────┘";
    return result;
}
std::tuple<std::uint32_t, bolt::ErrorCode> ReadPageSize(bolt::impl::File &file) {
    std::vector<std::byte> buf;
    buf.resize(4096);
    if (auto [s, err] = file.ReadAt(buf, 0); err != bolt::ErrorCode::Success) {
        return {0, err};
    }
    bolt::impl::meta *meta =
        reinterpret_cast<bolt::impl::meta *>(buf.data() + bolt::impl::pageHeaderSize);
    return {meta->pageSize, bolt::ErrorCode::Success};
}

std::tuple<bolt::impl::page *, std::vector<std::byte>, bolt::ErrorCode>
ReadPage(bolt::impl::File &file, bolt::impl::pgid pageid) {
    std::vector<std::byte> result;
    auto [pageSize, err] = ReadPageSize(file);
    if (err != bolt::ErrorCode::Success) {
        return {nullptr, std::vector<std::byte>(), err};
    }
    result.resize(pageSize);
    if (auto [n, err] = file.ReadAt(result, pageid * pageSize); err != bolt::ErrorCode::Success) {
        return {nullptr, std::vector<std::byte>(), err};
    }
    return {reinterpret_cast<bolt::impl::page *>(result.data()), result, bolt::ErrorCode::Success};
}

int Help() {
    auto help = R"(Bolt is a tool for inspecting bolt databases.

Usage:

    bolt-cli command [arguments]

The commands are:

    check       verifies integrity of bolt database
    compact     copies a bolt database, compacting it in the process
    info        print basic info
    help        print this screen
    page        prints one or more pages in human readable format
    pages       print list of pages with their types
    stats       iterate over all pages and generate usage stats

Use "bolt [command] --help" for more information about a command.)";
    fmt::print("{}", help);
    return 0;
}

int Check(int argc, char **argv) {
    auto help = R"(usage: bolt-cli check --path <PATH>

Check opens a database at PATH and runs an exhaustive check to verify that
all pages are accessible or are marked as freed. It also verifies that no
pages are double referenced.

Verification errors will stream out as they are found and the process will
return after all pages have been checked.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    return 0;
}
int Compact(int argc, char **argv) {
    auto help = R"(usage: bolt compact [options] --output <DST> --input <SRC>

Compact opens a database at SRC path and walks it recursively, copying keys
as they are found from all buckets, to a newly created database at DST path.

The original database is left untouched.

Additional options include:

    --tx-max-size NUM
        Specifies the maximum size of individual transactions.
        Defaults to 64KB.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    return 0;
}

int Info(int argc, char **argv) {
    auto help = R"(usage: bolt-cli info --path <PATH>

Info prints basic information about the Bolt database at PATH.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    return 0;
}

int Page(int argc, char **argv) {
    auto help =
        R"(usage: bolt-cli page --path <PATH> --page <pageid> [pageid...]

Page prints one or more pages in human readable format.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    return 0;
}

int Pages(int argc, char **argv) {
    auto help = R"(usage: bolt-cli pages --path <PATH>

Pages prints a table of pages with their type (meta, leaf, branch, freelist).
Leaf and branch pages will show a key count in the "items" column while the
freelist will show the number of free pages in the "items" column.

The "overflow" column shows the number of blocks that the page spills over
into. Normally there is no overflow but large keys and values can cause
a single page to take up multiple blocks.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    return 0;
}

int Stats(int argc, char **argv) {
    auto help = R"(usage: bolt stats --path <PATH>

Stats performs an extensive search of the database to track every page
reference. It starts at the current meta page and recursively iterates
through every accessible bucket.

The following errors can be reported:

    already freed
        The page is referenced more than once in the freelist.

    unreachable unfreed
        The page is not referenced by a bucket or in the freelist.

    reachable freed
        The page is referenced by a bucket but is also in the freelist.

    out of bounds
        A page is referenced that is above the high water mark.

    multiple references
        A page is referenced by more than one other page.

    invalid type
        The page type is not "meta", "leaf", "branch", or "freelist".

No errors should occur in your database.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    auto path = GetArgument<std::string>(cmd, "path");
    if (!path.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    bolt::DB db;
    if (auto err = db.Open(*path, true); err != bolt::ErrorCode::Success) {
        fmt::println("{}", err);
        return -1;
    }

    if (auto err = db.View([](bolt::Tx tx) -> bolt::ErrorCode {
            bolt::BucketStats s;
            int count = 0;
            if (auto err =
                    tx.ForEach([&](bolt::const_bytes name, bolt::Bucket b) -> bolt::ErrorCode {
                        s += b.Stats();
                        count += 1;
                        return bolt::ErrorCode::Success;
                    });
                err != bolt::ErrorCode::Success) {
                return err;
            }
            fmt::println("Aggregate statistics for {} buckets", count);
            fmt::println("Page count statistics");
            fmt::println("\tNumber of logical branch pages: {}", s.BranchPageN);
            fmt::println("\tNumber of physical branch overflow pages: {}", s.BranchOverflowN);
            fmt::println("\tNumber of logical leaf pages: {}", s.LeafPageN);
            fmt::println("\tNumber of physical leaf overflow pages: {}", s.LeafOverflowN);
            fmt::println("Tree statistics");
            fmt::println("\tNumber of keys/value pairs: {}", s.KeyN);
            fmt::println("\tNumber of levels in B+tree: {}", s.Depth);
            fmt::println("Page size utilization");
            fmt::println("\tBytes allocated for physical branch pages: {}", s.BranchAlloc);
            float percentage = 0;
            if (s.BranchAlloc != 0) {
                percentage =
                    static_cast<float>(s.BranchInuse) * 100.0 / static_cast<float>(s.BranchAlloc);
            }
            fmt::println("\tBytes actually used for branch data: {} ({:.02}%)", s.BranchInuse,
                         percentage);
            fmt::println("Bucket statistics");
            fmt::println("\tTotal number of buckets: {}", s.BucketN);
            percentage = 0;
            if (s.BucketN != 0) {
                percentage =
                    static_cast<float>(s.InlineBucketN) * 100.0 / static_cast<float>(s.BucketN);
            }
            fmt::println("\tTotal number on inlined buckets: {} ({:.02}%)", s.InlineBucketN,
                         percentage);
            percentage = 0;
            if (s.LeafInuse != 0) {
                percentage = static_cast<float>(s.InlineBucketInuse) * 100.0 /
                             static_cast<float>(s.LeafInuse);
            }
            fmt::println("\tBytes used for inlined buckets: {} ({:.02}%)", s.InlineBucketInuse,
                         percentage);
            return bolt::ErrorCode::Success;
        });
        err != bolt::ErrorCode::Success) {
        fmt::println("{}", err);
        return -1;
    }
    return 0;
}

int Dump(int argc, char **argv) {
    auto help = R"(usage: bolt dump --page <PAGEID> --path <PATH>

Dump prints a hexadecimal dump of a single page.)";
    auto cmd = Parse(argc, argv);
    if (auto val = GetArgument<std::nullptr_t>(cmd, "help"); val.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    auto path = GetArgument<std::string>(cmd, "path");
    auto pageid = GetArgument<long long>(cmd, "page");
    if (!path.has_value() || !pageid.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
    bolt::impl::File file;
    if (auto err = file.Open(*path, true); err != bolt::Success) {
        fmt::println("open file fail, {}", err);
        return -1;
    }
    auto [page, buf, err] = ReadPage(file, *pageid);
    if (err != bolt::Success) {
        fmt::println("read page fail, {}", err);
        return -1;
    }
    std::uint32_t pageSize;
    std::tie(pageSize, err) = ReadPageSize(file);
    fmt::println("{}", hexdump(pageSize * (*pageid), buf));
    return 0;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        std::string command = argv[1];
        if (command == "check") {
            return Check(argc - 2, argv + 2);
        } else if (command == "compact") {
            return Compact(argc - 2, argv + 2);
        } else if (command == "info") {
            return Info(argc - 2, argv + 2);
        } else if (command == "help") {
            return Help();
        } else if (command == "pages") {
            return Pages(argc - 2, argv + 2);
        } else if (command == "stats") {
            return Stats(argc - 2, argv + 2);
        } else if (command == "dump") {
            return Dump(argc - 2, argv + 2);
        } else {
            return Help();
        }
    } else {
        return Help();
    }
}
