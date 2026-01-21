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
TestResult TestCursor_Seek();
TestResult TestTx_Commit_ErrorTxClosed();
TestResult TestTx_Rollback_ErrorTxClosed();
TestResult TestTx_Commit_ErrorTxNotWritable();
TestResult TestTx_Cursor();
TestResult TestTx_CreateBucket_ErrorTxNotWritable();
TestResult TestTx_CreateBucket_ErrorTxClosed();
TestResult TestTx_Bucket();
TestResult TestTx_Get_NotFound();
TestResult TestTx_CreateBucket();
TestResult TestTx_CreateBucketIfNotExists();
TestResult TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired();
TestResult TestTx_CreateBucket_ErrorBucketExists();
TestResult TestTx_DeleteBucket();
TestResult TestTx_DeleteBucket_ErrorTxClosed();
TestResult TestTx_DeleteBucket_ReadOnly();
TestResult TestTx_DeleteBucket_NotFound();
TestResult TestTx_ForEach_NoError();
TestResult TestTx_ForEach_WithError();
TestResult TestTx_OnCommit();
TestResult TestTx_OnCommit_Rollback();
TestResult TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired();

static const std::vector<Test> tests = {
    // {"Test Page Type", TestPageType},
    // {"Test Merge Pgid", TestMergePgid},
    // {"Test Freelist free", TestFreelist_free},
    // {"Test Freelist free overflow", TestFreelist_free_overflow},
    // {"Test Freelist release", TestFreelist_release},
    // {"Test Freelist allocate", TestFreelist_allocate},
    // {"Test Freelist read", TestFreelist_read},
    // {"Test Freelist write", TestFreelist_write},
    // {"Test Node put", TestNode_put},
    // {"Test Node read_LeafPage", TestNode_read_LeafPage},
    // {"Test Node write_LeafPage", TestNode_write_LeafPage},
    // {"Test Node split", TestNode_split},
    // {"Test Node split_MinKeys", TestNode_split_MinKeys},
    // {"Test Node split_SinglePage", TestNode_split_SinglePage},
    // {"Test DB Open", TestDB_Open},
    // {"Test DB Open Path Required", TestDB_OpenPathRequired},
    // {"Test DB Open ErrorDatabaseInvalid", TestDB_OpenInvalid},
    // {"Test DB Open ErrorVersionMismatch", TestDB_OpenErrVersionMismatch},
    // {"Test DB Open ErrorChecksum", TestDB_OpenErrChecksum},
    // {"Test DB Open Size", TestDB_OpenSize},
    // {"Test Cursor Bucket", TestCursor_Bucket},
    // {"Test Cursor Seek", TestCursor_Seek},
    // {"Test Tx Commit ErrorTxClosed", TestTx_Commit_ErrorTxClosed},
    // {"Test Tx Rollback ErrorTxClosed", TestTx_Rollback_ErrorTxClosed},
    // {"Test Tx Commit ErrorTxNotWritable", TestTx_Commit_ErrorTxNotWritable},
    // {"Test Tx Cursor", TestTx_Cursor},
    // {"Test Tx CreateBucket ErrorTxNotWritable",
    //  TestTx_CreateBucket_ErrorTxNotWritable},
    // {"Test Tx CreateBucket ErrorTxClosed",
    //  TestTx_CreateBucket_ErrorTxClosed},
    // {"Test Tx Bucket", TestTx_Bucket},
    // {"Test Tx Get NotFound", TestTx_Get_NotFound},
    // {"Test Tx CreateBucket", TestTx_CreateBucket},
    // {"Test Tx CreateBucketIfNotExists", TestTx_CreateBucketIfNotExists},
    // {"Test Tx CreateBucketIfNotExists ErrorBucketNameRequired",
    //  TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired},
    // {"Test Tx CreateBucket ErrorBucketExists",
    //  TestTx_CreateBucket_ErrorBucketExists},
    // {"Test Tx CreateBucket ErrorBucketNameRequired",
    //  TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired},
    // {"Test Tx DeleteBucket", TestTx_DeleteBucket},
    {"Test Tx DeleteBucket ErrorTxClosed",
     TestTx_DeleteBucket_ErrorTxClosed},
    // {"Test Tx DeleteBucket ReadOnly", TestTx_DeleteBucket_ReadOnly},
    // {"Test Tx DeleteBucket NotFound", TestTx_DeleteBucket_NotFound},
    // {"Test Tx ForEach NoError", TestTx_ForEach_NoError},
    // {"Test Tx ForEach WithError", TestTx_ForEach_WithError},
    // {"Test Tx OnCommit", TestTx_OnCommit},
    // {"Test Tx OnCommit Rollback", TestTx_OnCommit_Rollback},
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

#if 0
class Intvec {
  public:
    explicit Intvec(size_t num = 0) : m_size(num), m_data(std::make_unique<int[]>(10)) {
        log("constructor");
    }

    ~Intvec() {
        log("destructor");
        if (m_data) {
            m_data.release();
            m_data = 0;
        }
    }

    Intvec(const Intvec &other)
        : m_size(other.m_size), m_data(new int[m_size]) {
        log("copy constructor");
        for (size_t i = 0; i < m_size; ++i)
            m_data[i] = other.m_data[i];
    }

    Intvec &operator=(const Intvec &other) {
        log("copy assignment operator");
        Intvec tmp(other);
        std::swap(m_size, tmp.m_size);
        std::swap(m_data, tmp.m_data);
        return *this;
    }

    Intvec &operator=(Intvec &&other) {
        log("move assignment operator");
        std::swap(m_size, other.m_size);
        std::swap(m_data, other.m_data);
        return *this;
    }

  private:
    void log(const char *msg) {
        fmt::println("{} {}", fmt::ptr(this), msg);
    }

    size_t m_size;
    std::unique_ptr<int[]> m_data;
};

int main(int argc, char **argv) {
    Intvec v1(20);
    Intvec v2;

    fmt::println("assigning lvalue...");
    v2 = v1;
    fmt::println("ended assigning lvalue...");

    fmt::println("assigning rvalue...");
    v2 = Intvec(33);
    fmt::println("ended assigning rvalue...");
}
#endif
