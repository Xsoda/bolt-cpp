#ifndef __FREELIST_HPP__
#define __FREELIST_HPP__

#include "impl/utils.hpp"

namespace bolt::impl {

struct page;

void mergepgids(std::span<impl::pgid> dest, std::span<impl::pgid> a,
                std::span<impl::pgid> b);

struct freelist {
    std::vector<impl::pgid> ids;
    std::map<impl::txid, std::vector<impl::pgid>> pending;
    std::map<impl::pgid, bool> cache;

    size_t size();
    size_t count();
    size_t free_count();
    size_t pending_count();
    void copyall(std::span<impl::pgid> dest);
    impl::pgid allocate(size_t n);
    void free(impl::txid txid, impl::page *p);
    void rollback(impl::txid txid);
    void release(impl::txid txid);
    bool freed(impl::pgid pgid);
    void read(impl::page *p);
    bolt::ErrorCode write(impl::page *p);
    void reload(impl::page *p);
    void reindex();
};
}
#endif  // !__FREELIST_HPP__
