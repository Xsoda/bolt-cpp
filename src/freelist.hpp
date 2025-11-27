#ifndef __FREELIST_HPP__
#define __FREELIST_HPP__

#include "common.hpp"
#include "error.hpp"

namespace bolt {

struct page;

struct freelist {
    std::vector<bolt::pgid> ids;
    std::map<bolt::txid, std::vector<bolt::pgid>> pending;
    std::map<bolt::pgid, bool> cache;

    int size();
    int count();
    int free_count();
    int pending_count();
    void copyall(std::span<bolt::pgid> &dest);
    bolt::pgid allocate(int n);
    void free(bolt::txid txid, bolt::page *p);
    void rollback(bolt::txid txid);
    void release(bolt::txid txid);
    bool freed(bolt::pgid pgid);
    void read(bolt::page *p);
    bolt::ErrorCode write(bolt::page *p);
    void reload(bolt::page *p);
    void reindex();
};

}
#endif  // !__FREELIST_HPP__
