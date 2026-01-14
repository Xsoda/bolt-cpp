#pragma once

#ifndef __ERROR_HPP__
#define __ERROR_HPP__

namespace bolt {

#define ERROR_MAP(XX)                                                          \
  XX(DatabaseNotOpen, "database not open")                                     \
  XX(DatabaseAlreadyOpen, "database already open")                             \
  XX(DatabaseInvalid, "invalid database")                                      \
  XX(VersionMismatch, "version mismatch")                                      \
  XX(Checksum, "checksum error")                                               \
  XX(Timeout, "timeout")                                                       \
  XX(TxNotWritable, "tx not writable")                                         \
  XX(TxClosed, "tx closed")                                                    \
  XX(DatabaseReadOnly, "database is in read-only mode")                        \
  XX(BucketNotFound, "bucket not found")                                       \
  XX(BucketExists, "bucket already exists")                                    \
  XX(BucketNameRequired, "bucket name required")                               \
  XX(KeyRequired, "key required")                                              \
  XX(KeyTooLarge, "key too large")                                             \
  XX(ValueTooLarge, "value too large")                                         \
  XX(IncompatiableValue, "incompatible value")                                 \
  XX(SystemCall, "system call failure")                                        \
  XX(TrySolo, "batch function returned an error and should be re-run solo")    \
  XX(FileResizeFail, "file resize fail")                                       \
  XX(FileSyncFail, "file sync fail")                                           \
  XX(FileSizeTooSmall, "file size too small")                                  \
  XX(MmapTooLarge, "mmap too large")                                           \
  XX(DatabaseEOF, "database eof")                                              \
  XX(Unexpected, "unexpected")

typedef enum {
  Success = 0,

#define XX(name, desc) Error ##name,
  ERROR_MAP(XX)
#undef XX

  MaxErrorCode,
} ErrorCode;


}

#endif  // !__ERROR_HPP__
