#include "test.hpp"
#include <chrono>

TestResult TestPageType();
TestResult TestMergePgid();
TestResult TestFreelist_free();
TestResult TestFreelist_free_overflow();
TestResult TestFreelist_release();
TestResult TestFreelist_allocate();
TestResult TestFreelist_read();
TestResult TestFreelist_write();
TestResult TestNode_put();
TestResult TestNode_read_LeafPage();
TestResult TestNode_write_LeafPage();
TestResult TestNode_split();
TestResult TestNode_split_MinKeys();
TestResult TestNode_split_SinglePage();
TestResult TestDB_Open();
TestResult TestDB_OpenPathRequired();
TestResult TestDB_OpenInvalid();
TestResult TestDB_OpenErrVersionMismatch();
TestResult TestDB_OpenErrChecksum();
// TestResult TestDB_OpenSize();
TestResult TestCursor_Bucket();

static const std::vector<Test> tests = {
    {"Test Page Type", TestPageType},
    {"Test Merge Pgid", TestMergePgid},
    {"Test Freelist free", TestFreelist_free},
    {"Test Freelist free overflow", TestFreelist_free_overflow},
    {"Test Freelist release", TestFreelist_release},
    {"Test Freelist allocate", TestFreelist_allocate},
    {"Test Freelist read", TestFreelist_read},
    {"Test Freelist write", TestFreelist_write},
    {"Test Node put", TestNode_put},
    {"Test Node read_LeafPage", TestNode_read_LeafPage},
    {"Test Node write_LeafPage", TestNode_write_LeafPage},
    {"Test Node split", TestNode_split},
    {"Test Node split_MinKeys", TestNode_split_MinKeys},
    {"Test Node split_SinglePage", TestNode_split_SinglePage},
    {"Test DB Open", TestDB_Open},
    {"Test DB Open Path Required", TestDB_OpenPathRequired},
    {"Test DB Open ErrorDatabaseInvalid", TestDB_OpenInvalid},
    {"Test DB Open ErrorVersionMismatch", TestDB_OpenErrVersionMismatch},
    {"Test DB Open ErrorChecksum", TestDB_OpenErrChecksum},
    // {"Test DB Open Size", TestDB_OpenSize},
    {"Test Cursor Bucket", TestCursor_Bucket},
};

int main(int argc, char **argv) {
    int success_tests = 0;
    int failed_tests = 0;
    std::chrono::steady_clock::time_point startTime, endTime;
    startTime = std::chrono::steady_clock::now();
    for (auto test : tests) {
        auto res = test.run();
        if (res.success) {
            success_tests++;
        } else {
            failed_tests++;
        }
    }
    endTime = std::chrono::steady_clock::now();

    auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    auto durationS =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
    fmt::println("Finished {} tests in {}s ({}ms). Succeed: {}. Failed: {}.",
                 success_tests + failed_tests, durationS.count(),
                 durationMs.count(), success_tests, failed_tests);
    return 0;
}
