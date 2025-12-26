#ifndef __META_HPP__
#define __META_HPP__

#include "common.hpp"
#include "bucket.hpp"

namespace bolt {

struct page;

struct meta {
    std::uint32_t magic;
    std::uint32_t version;
    std::uint32_t pageSize;
    std::uint32_t flags;
    bolt::bucket root;
    bolt::pgid freelist;
    bolt::pgid pgid;
    bolt::txid txid;
    std::uint64_t checksum;

    meta(bolt::pgid id) : pgid(id){};
    void copy(bolt::meta *dest) const;
    void write(bolt::page *page);
    std::uint64_t sum64();
    int validate();
};

}

#endif  // !__META_HPP__
