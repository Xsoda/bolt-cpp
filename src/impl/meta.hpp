#ifndef __META_HPP__
#define __META_HPP__

#include "impl/bucket.hpp"
#include "impl/utils.hpp"

namespace bolt::impl {

struct page;

struct meta {
    std::uint32_t magic;
    std::uint32_t version;
    std::uint32_t pageSize;
    std::uint32_t flags;
    impl::bucket root;
    impl::pgid freelist;
    impl::pgid pgid;
    impl::txid txid;
    std::uint64_t checksum;

    explicit meta();
    explicit meta(impl::pgid id);
    void copy(impl::meta *dest) const;
    void write(impl::page *page);
    std::uint64_t sum64();
    bolt::ErrorCode validate();
};

}

#endif  // !__META_HPP__
