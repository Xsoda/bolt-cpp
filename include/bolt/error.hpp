#pragma once


#ifndef __ERROR_HPP__
#define __ERROR_HPP__

#include "fmt/base.h"
#include "fmt/format.h"

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
  XX(Unexpected, "unexpected")                                                 \
  XX(Expected, "expected error")

typedef enum {
  Success = 0,

#define XX(name, desc) Error ##name,
  ERROR_MAP(XX)
#undef XX

  MaxErrorCode,
} ErrorCode;


} // namespace bolt

FMT_BEGIN_NAMESPACE
template <> struct formatter<bolt::ErrorCode>: nested_formatter<const char *> {
  auto format(bolt::ErrorCode error_code, format_context &ctx) const
      -> decltype(ctx.out()) {
    return write_padded(ctx, [this, error_code](auto out) -> decltype(out) {
        switch (error_code) {
#define XX(name, desc)                                                         \
            case bolt::ErrorCode::Error##name:                          \
                return fmt::format_to(out, "({}) - {}", #name, desc);
        ERROR_MAP(XX)
#undef XX
        case bolt::ErrorCode::Success:
            return fmt::format_to(out, "({}) - {}", "Success", "operation success");
        case bolt::ErrorCode::MaxErrorCode:
            return fmt::format_to(out, "{}", "impossible error code");
        }
        return fmt::format_to(out, "impossible");
    });
  };
};

FMT_END_NAMESPACE

#endif  // !__ERROR_HPP__
