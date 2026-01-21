#pragma once

#ifndef __BOLT_HPP__
#define __BOLT_HPP__

#include "error.hpp"
#include "pimpl.hpp"

namespace bolt {
    namespace impl {
        struct DB;
        struct Cursor;
        struct Tx;
        struct Bucket;

        using DBPtr = std::shared_ptr<DB>;
        using TxPtr = std::shared_ptr<Tx>;
        using BucketPtr = std::shared_ptr<Bucket>;
        using CursorPtr = std::shared_ptr<Cursor>;
    } // namespace impl
    class DB;
    class Bucket;
    class Tx;
    class Cursor;

    using DBPtr = std::shared_ptr<DB>;
    using TxPtr = std::shared_ptr<Tx>;
    using BucketPtr = std::shared_ptr<Bucket>;
    using CursorPtr = std::shared_ptr<Cursor>;

class DB : public std::enable_shared_from_this<DB> {
public:
    std::shared_ptr<DB> Open(std::string path, bool readOnly = false);
    std::string Path() const;
    bolt::ErrorCode Close();

private:
    bolt::impl::DBPtr pImpl;
};

class Bucket {
public:
private:
    bolt::impl::BucketPtr pImpl;
};

class Cursor {
public:
private:
    bolt::impl::CursorPtr pImpl;
};

class Tx {
public:
private:
    bolt::impl::TxPtr pImpl;
};
}
#endif //__BOLT_HPP__
