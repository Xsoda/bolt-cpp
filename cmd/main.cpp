#include "args.hpp"

int Help() {
    auto help = R"(
Bolt is a tool for inspecting bolt databases.

Usage:

    bolt-cli command [arguments]

The commands are:

    check       verifies integrity of bolt database
    compact     copies a bolt database, compacting it in the process
    info        print basic info
    help        print this screen
    pages       print list of pages with their types
    stats       iterate over all pages and generate usage stats

Use "bolt [command] -h" for more information about a command.
)";
    fmt::print("{}", help);
    return 0;
}

int Check(int argc, char **argv) {
    auto help = R"(
usage: bolt-cli check --path <PATH>

Check opens a database at PATH and runs an exhaustive check to verify that
all pages are accessible or are marked as freed. It also verifies that no
pages are double referenced.

Verification errors will stream out as they are found and the process will
return after all pages have been checked.
)";
    return 0;
}
int Compact(int argc, char **argv) {
    auto help = R"(
usage: bolt compact [options] --output <DST> --input <SRC>

Compact opens a database at SRC path and walks it recursively, copying keys
as they are found from all buckets, to a newly created database at DST path.

The original database is left untouched.

Additional options include:

    --tx-max-size NUM
        Specifies the maximum size of individual transactions.
        Defaults to 64KB.
)";
    return 0;
}

int Info(int argc, char **argv) {
    auto help = R"(
usage: bolt-cli info --path <PATH>

Info prints basic information about the Bolt database at PATH.
)";
    return 0;
}

int Page(int argc, char **argv) {
    auto help = R"(
usage: bolt-cli page -page PATH pageid [pageid...]

Page prints one or more pages in human readable format.
)";
    return 0;
}

int Pages(int argc, char **argv) {
    auto help = R"(
usage: bolt-cli pages PATH

Pages prints a table of pages with their type (meta, leaf, branch, freelist).
Leaf and branch pages will show a key count in the "items" column while the
freelist will show the number of free pages in the "items" column.

The "overflow" column shows the number of blocks that the page spills over
into. Normally there is no overflow but large keys and values can cause
a single page to take up multiple blocks.
)";
    return 0;
}

int Stats(int argc, char **argv) {
    auto help = R"(
usage: bolt stats PATH

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

No errors should occur in your database.
)";

    return 0;
}

int Dump(int argc, char **argv) {
    auto help = R"(
usage: bolt dump --page <PAGEID> --path <PATH>

Dump prints a hexadecimal dump of a single page.
)";
    auto cmd = Parse(argc, argv);
    auto path = GetArgument<std::string>(cmd, "path");
    auto page = GetArgument<long long>(cmd, "page");
    if (!path.has_value() || !page.has_value()) {
        fmt::println("{}", help);
        return 0;
    }
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
