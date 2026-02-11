#include "fmt/base.h"
#include "impl/bsearch.hpp"
#include "bolt/error.hpp"
#include "test.hpp"
#include "util.hpp"
#include <algorithm>
#include <chrono>
#include <compare>
#include <span>
#include <string>
#include <vector>

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
TestResult TestDB_OpenSize();
TestResult TestDB_Open_Size_Large();
TestResult TestDB_Open_Check();
TestResult TestDB_Open_FileTooSmall();
TestResult TestDB_Open_InitialMmapSize();
TestResult TestDB_Begin_ErrDatabaseNotOpen();
TestResult TestDB_BeginRW();
TestResult TestDB_BeginRW_Closed();
TestResult TestDB_Close_PendingTx_RW();
TestResult TestDB_Close_PendingTx_RO();
TestResult TestDB_Update();
TestResult TestDB_Update_Closed();
TestResult TestDB_Update_ManualCommit();
TestResult TestDB_Update_ManualRollback();
TestResult TestDB_View_ManualCommit();
TestResult TestDB_View_ManualRollback();
TestResult TestDB_Update_Panic();
TestResult TestDB_View_Error();
TestResult TestDB_View_Panic();
TestResult TestDB_Stats();
TestResult TestDB_Consistency();
TestResult TestDB_Stats_Sub();
TestResult TestDB_Batch();
TestResult TestDB_Batch_Panic();
TestResult TestDB_BatchFull();
TestResult TestDB_BatchTime();
TestResult TestCursor_Bucket();
TestResult TestCursor_Seek();
TestResult TestCursor_Delete();
TestResult TestCursor_Seek_Large();
TestResult TestCursor_EmptyBucket();
TestResult TestCursor_EmptyBucketReverse();
TestResult TestCursor_Iterate_Leaf();
TestResult TestCursor_LeafRootReverse();
TestResult TestCursor_Restart();
TestResult TestCursor_First_EmptyPages();
TestResult TestCursor_QuickCheck();
TestResult TestCursor_QuickCheck_Reverse();
TestResult TestCursor_QuickCheck_BucketsOnly();
TestResult TestCursor_QuickCheck_BucketsOnly_Reverse();
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
TestResult TestBucket_Get_NonExistent();
TestResult TestBucket_Get_FromNode();
TestResult TestBUcket_Get_IncompatibleValue();
TestResult TestBucket_Get_Capacity();
TestResult TestBucket_Put();
TestResult TestBucket_Put_Repeat();
TestResult TestBucket_Put_Large();
TestResult TestBucket_Put_VeryLarge();
TestResult TestBUcket_Put_IncompatibleValue();
TestResult TestBucket_Put_Closed();
TestResult TestBucket_Put_ReadOnly();
TestResult TestBucket_Delete();
TestResult TestBucket_Delete_Large();
TestResult TestBucket_Delete_FreelistOverflow();
TestResult TestBucket_Nested();
TestResult TestBucket_Delete_Bucket();
TestResult TestBucket_Delete_ReadOnly();
TestResult TestBucket_Delete_Closed();
TestResult TestBucket_DeleteBucket_Nested();
TestResult TestBucket_DeleteBucket_Nested2();
TestResult TestBucket_DeleteBucket_Large();
TestResult TestBucket_Bucket_IncompatibleValue();
TestResult TestBucket_CreateBucket_IncompatibleValue();
TestResult TestBucket_DeleteBucket_IncompatibleValue();
TestResult TestBucket_Sequence();
TestResult TestBucket_NextSequence();
TestResult TestBucket_NextSequence_Persist();
TestResult TestBucket_NextSequence_ReadOnly();
TestResult TestBucket_NextSequence_Closed();
TestResult TestBucket_ForEach();
TestResult TestBucket_ForEach_ShortCircuit();
TestResult TestBucket_ForEach_Closed();
TestResult TestBucket_Put_EmptyKey();
TestResult TestBucket_Put_KeyTooLarge();
TestResult TestBucket_Put_ValueTooLarge();
TestResult TestBucket_Stats();
TestResult TestBucket_Stats_RandomFill();
TestResult TestBucket_Stats_Small();
TestResult TestBucket_Stats_EmptyBucket();
TestResult TestBucket_Stats_Nested();
TestResult TestBucket_Stats_Large();
TestResult TestBucket_Put_Single();
TestResult TestBucket_Put_Multiple();
TestResult TestBucket_Delete_Quick();

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
    {"Test DB Open Size", TestDB_OpenSize},
    {"Test DB Open Size Large", TestDB_Open_Size_Large},
    {"Test DB Open Check", TestDB_Open_Check},
    {"Test DB Open_FileTooSmall", TestDB_Open_FileTooSmall},
    {"Test DB Open InitialMmapSize", TestDB_Open_InitialMmapSize},
    {"Test DB Begin ErrDatabaseNotOpen},", TestDB_Begin_ErrDatabaseNotOpen},
    {"Test DB BeginRW", TestDB_BeginRW},
    {"Test DB BeginRW Closed", TestDB_BeginRW_Closed},
    {"Test DB Close PendingTx RW", TestDB_Close_PendingTx_RW},
    {"Test DB Close PendingTx RO", TestDB_Close_PendingTx_RO},
    {"Test DB Update", TestDB_Update},
    {"Test DB Update Closed", TestDB_Update_Closed},
    {"Test DB Update ManualCommit", TestDB_Update_ManualCommit},
    {"Test DB Update ManualRollback", TestDB_Update_ManualRollback},
    {"Test DB View ManualCommit", TestDB_View_ManualCommit},
    {"Test DB View ManualRollback", TestDB_View_ManualRollback},
    {"Test DB Update Panic", TestDB_Update_Panic},
    {"Test DB View Error", TestDB_View_Error},
    {"Test DB View Panic", TestDB_View_Panic},
    {"Test DB Stats", TestDB_Stats},
    {"Test DB Consistency", TestDB_Consistency},
    {"Test DB Stats Sub", TestDB_Stats_Sub},
    {"Test DB Batch", TestDB_Batch},
    {"Test DB Batch Panic", TestDB_Batch_Panic},
    {"Test DB BatchFull", TestDB_BatchFull},
    {"Test DB BatchTime", TestDB_BatchTime},
    {"Test Cursor Bucket", TestCursor_Bucket},
    {"Test Cursor Seek", TestCursor_Seek},
    {"Test Cursor Delete", TestCursor_Delete},
    {"Test Cursor Seek Large", TestCursor_Seek_Large},
    {"Test Cursor EmptyBucket", TestCursor_EmptyBucket},
    {"Test Cursor EmptyBucketReverse", TestCursor_EmptyBucketReverse},
    {"Test Cursor Iterate Leaf", TestCursor_Iterate_Leaf},
    {"Test Cursor LeafRootReverse", TestCursor_LeafRootReverse},
    {"Test Cursor Restart", TestCursor_Restart},
    {"Test Cursor First EmptyPages", TestCursor_First_EmptyPages},
    {"Test Cursor QuickCheck", TestCursor_QuickCheck},
    {"Test Cursor QuickCheck Reverse", TestCursor_QuickCheck_Reverse},
    {"Test Cursor QuickCheck BucketsOnly", TestCursor_QuickCheck_BucketsOnly},
    {"Test Cursor QuickCheck BucketsOnly Reverse",
     TestCursor_QuickCheck_BucketsOnly_Reverse},
    {"Test Tx Commit ErrorTxClosed", TestTx_Commit_ErrorTxClosed},
    {"Test Tx Rollback ErrorTxClosed", TestTx_Rollback_ErrorTxClosed},
    {"Test Tx Commit ErrorTxNotWritable", TestTx_Commit_ErrorTxNotWritable},
    {"Test Tx Cursor", TestTx_Cursor},
    {"Test Tx CreateBucket ErrorTxNotWritable",
     TestTx_CreateBucket_ErrorTxNotWritable},
    {"Test Tx CreateBucket ErrorTxClosed", TestTx_CreateBucket_ErrorTxClosed},
    {"Test Tx Bucket", TestTx_Bucket},
    {"Test Tx Get NotFound", TestTx_Get_NotFound},
    {"Test Tx CreateBucket", TestTx_CreateBucket},
    {"Test Tx CreateBucketIfNotExists", TestTx_CreateBucketIfNotExists},
    {"Test Tx CreateBucketIfNotExists ErrorBucketNameRequired",
     TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired},
    {"Test Tx CreateBucket ErrorBucketExists",
     TestTx_CreateBucket_ErrorBucketExists},
    {"Test Tx CreateBucket ErrorBucketNameRequired",
     TestTx_CreateBucketIfNotExists_ErrorBucketNameRequired},
    {"Test Tx DeleteBucket", TestTx_DeleteBucket},
    {"Test Tx DeleteBucket ErrorTxClosed", TestTx_DeleteBucket_ErrorTxClosed},
    {"Test Tx DeleteBucket ReadOnly", TestTx_DeleteBucket_ReadOnly},
    {"Test Tx DeleteBucket NotFound", TestTx_DeleteBucket_NotFound},
    {"Test Tx ForEach NoError", TestTx_ForEach_NoError},
    {"Test Tx ForEach WithError", TestTx_ForEach_WithError},
    {"Test Tx OnCommit", TestTx_OnCommit},
    {"Test Tx OnCommit Rollback", TestTx_OnCommit_Rollback},
    {"Test Bucket Get NonExistent", TestBucket_Get_NonExistent},
    {"Test Bucket Get FromNode", TestBucket_Get_FromNode},
    {"Test BUcket Get IncompatibleValue", TestBUcket_Get_IncompatibleValue},
    {"Test Bucket Get Capacity", TestBucket_Get_Capacity},
    {"Test Bucket Put", TestBucket_Put},
    {"Test Bucket Put Repeat", TestBucket_Put_Repeat},
    {"Test Bucket Put Large", TestBucket_Put_Large},
    {"Test Bucket Put VeryLarge", TestBucket_Put_VeryLarge},
    {"Test BUcket Put IncompatibleValue", TestBUcket_Put_IncompatibleValue},
    {"Test Bucket Put Closed", TestBucket_Put_Closed},
    {"Test Bucket Put ReadOnly", TestBucket_Put_ReadOnly},
    {"Test Bucket Delete", TestBucket_Delete},
    {"Test Bucket Delete Large", TestBucket_Delete_Large},
    {"Test Bucket Delete FreelistOverflow", TestBucket_Delete_FreelistOverflow},
    {"Test Bucket Nested", TestBucket_Nested},
    {"Test Bucket Delete Bucket", TestBucket_Delete_Bucket},
    {"Test Bucket Delete ReadOnly", TestBucket_Delete_ReadOnly},
    {"Test Bucket Delete Closed", TestBucket_Delete_Closed},
    {"Test Bucket DeleteBucket Nested", TestBucket_DeleteBucket_Nested},
    {"Test Bucket DeleteBucket Nested2", TestBucket_DeleteBucket_Nested2},
    {"Test Bucket DeleteBucket Large", TestBucket_DeleteBucket_Large},
    {"Test Bucket Bucket IncompatibleValue",
     TestBucket_Bucket_IncompatibleValue},
    {"Test Bucket CreateBucket IncompatibleValue",
     TestBucket_CreateBucket_IncompatibleValue},
    {"Test Bucket DeleteBucket IncompatibleValue",
     TestBucket_DeleteBucket_IncompatibleValue},
    {"Test Bucket Sequence", TestBucket_Sequence},
    {"Test Bucket NextSequence", TestBucket_NextSequence},
    {"Test Bucket NextSequence Persist", TestBucket_NextSequence_Persist},
    {"Test Bucket NextSequence ReadOnly", TestBucket_NextSequence_ReadOnly},
    {"Test Bucket NextSequence Closed", TestBucket_NextSequence_Closed},
    {"Test Bucket ForEach", TestBucket_ForEach},
    {"Test Bucket ForEach ShortCircuit", TestBucket_ForEach_ShortCircuit},
    {"Test Bucket ForEach Closed", TestBucket_ForEach_Closed},
    {"Test Bucket Put EmptyKey", TestBucket_Put_EmptyKey},
    {"Test Bucket Put KeyTooLarge", TestBucket_Put_KeyTooLarge},
    {"Test Bucket Put ValueTooLarge", TestBucket_Put_ValueTooLarge},
    {"Test Bucket Stats", TestBucket_Stats},
    {"Test Bucket Stats RandomFill", TestBucket_Stats_RandomFill},
    {"Test Bucket Stats Small", TestBucket_Stats_Small},
    {"Test Bucket Stats EmptyBucket", TestBucket_Stats_EmptyBucket},
    {"Test Bucket Stats Nested", TestBucket_Stats_Nested},
    {"Test Bucket Stats Large", TestBucket_Stats_Large},
    {"Test Bucket Put Single", TestBucket_Put_Single},
    {"Test Bucket Put Multiple", TestBucket_Put_Multiple},
    {"Test Bucket Delete Quick", TestBucket_Delete_Quick},
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
