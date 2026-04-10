#include "impl/freelist.hpp"
#include "impl/page.hpp"
#include <algorithm>
#include <iostream>

namespace bolt::impl {

size_t freelist::size() {
    size_t n = this->count();
    if (n >= 0xFFFF) {
        n++;
    }
    return impl::pageHeaderSize + sizeof(impl::pgid) * n;
}

size_t freelist::count() { return this->free_count() + this->pending_count(); }

size_t freelist::free_count() { return this->ids.size(); }

size_t freelist::pending_count() {
    size_t count = 0;
    for (auto &it : pending) {
        count += it.second.size();
    }
    return count;
}

void mergepgids(std::span<impl::pgid> dest, std::span<impl::pgid> a, std::span<impl::pgid> b) {
    if (dest.size() < a.size() + b.size()) {
        _assert(false, "mergepgids bad length");
    }
    size_t length = 0;
    if (a.size() == 0) {
        std::copy(b.begin(), b.end(), dest.begin());
        return;
    }
    if (b.size() == 0) {
        std::copy(a.begin(), a.end(), dest.begin());
        return;
    }

    std::span<impl::pgid> lead{a};
    std::span<impl::pgid> follow{b};

    if (b[0] < a[0]) {
        std::swap(lead, follow);
    }

    while (lead.size() > 0) {
        auto it = std::find_if(lead.begin(), lead.end(),
                               [&](impl::pgid &item) -> bool { return item > follow[0]; });
        std::copy(lead.begin(), it, dest.begin() + length);
        length += std::distance(lead.begin(), it);

        if (it == lead.end()) {
            break;
        }
        lead = lead.subspan(std::distance(lead.begin(), it));
        std::swap(lead, follow);
    }
    std::copy(follow.begin(), follow.end(), dest.begin() + length);
}

void freelist::copyall(std::span<impl::pgid> dest) {
    std::vector<impl::pgid> m;
    for (auto &it : pending) {
        std::copy(it.second.begin(), it.second.end(), std::back_inserter(m));
    }
    std::sort(m.begin(), m.end());
    mergepgids(dest, ids, m);
}

// allocate returns the starting page id of a contiguous list of pages of a
// given size. If a contiguous block cannot be found then 0 is returned.
impl::pgid freelist::allocate(size_t n) {
    if (ids.size() == 0) {
        return 0;
    }
    impl::pgid initial = 0, previd = 0;
    for (size_t i = 0; i < ids.size(); i++) {
        impl::pgid id = ids[i];
        if (id <= 1) {
            _assert(false, "invalid page allocation: {}", id);
        }
        // Reset initial page if this is not contiguous.
        if (previd == 0 || id - previd != 1) {
            initial = id;
        }

        // If we found a contiguous block then remove it and return it.
        if (id - initial + 1 == (impl::pgid)n) {
            // If we're allocating off the beginning then take the fast path
            // and just adjust the existing slice. This will use extra memory
            // temporarily but the append() in free() will realloc the slice
            // as is necessary.
            if (i + 1 == n) {
                ids.erase(ids.begin(), ids.begin() + i + 1);
            } else {
                ids.erase(ids.begin() + i + 1 - n, ids.begin() + i + 1);
            }

            // Remove from the free cache.
            for (impl::pgid j = 0; j < (impl::pgid)n; j++) {
                auto it = cache.find(initial + j);
                if (it != cache.end()) {
                    cache.erase(it);
                }
            }

            return initial;
        }

        previd = id;
    }
    return 0;
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
void freelist::free(impl::txid txid, page *p) {
    if (p->id <= 1) {
        _assert(false, "cannot free page 0 or 1: {}", p->id);
    }

    // Free page and all its overflow pages.
    auto &ids = pending[txid];
    for (impl::pgid id = p->id; id <= p->id + p->overflow; id++) {
        // Verify that page is not already free.
        auto it = cache.find(id);
        _assert(it == cache.end(), "page {} already freed", id);

        // Add to the freelist and cache.
        ids.push_back(id);
        cache[id] = true;
    }
}

// release moves all page ids for a transaction id (or older) to the freelist.
void freelist::release(impl::txid txid) {
    std::vector<impl::pgid> m, merge;
    for (auto &[key, value] : pending) {
        if (key <= txid) {
            // Move transaction's pending pages to the available freelist.
            // Don't remove from the cache since the page is still free.
            std::copy(value.begin(), value.end(), std::back_inserter(m));
        }
    }
    std::erase_if(pending, [=](const auto &item) {
        const auto &[key, value] = item;
        return key <= txid;
    });
    std::sort(m.begin(), m.end());
    merge.assign(m.size() + ids.size(), 0);
    mergepgids(merge, ids, m);
    ids = merge;
}

void freelist::rollback(impl::txid txid) {
    auto it = pending.find(txid);
    if (it != pending.end()) {
        auto container = it->second;
        std::erase_if(cache, [&](const auto &item) {
            const auto &[key, value] = item;
            return std::find(container.begin(), container.end(), key) != container.end();
        });
        pending.erase(it);
    }
}

bool freelist::freed(impl::pgid pgid) {
    auto it = cache.find(pgid);
    if (it != cache.end()) {
        return it->second;
    }
    return false;
}

void freelist::read(impl::page *p) {
    int idx = 0;
    int count = (int)p->count;
    if (count == 0xFFFF) {
        idx = 1;
        count = (int)reinterpret_cast<impl::pgid *>(&p->ptr)[0];
    }

    ids.clear();
    if (count > 0) {
        impl::pgid *ptr = reinterpret_cast<impl::pgid *>(&p->ptr);
        std::span<impl::pgid> s(&ptr[idx], count);
        ids.assign(count, 0);
        std::copy(s.begin(), s.end(), ids.begin());
        std::sort(ids.begin(), ids.end());
    }
    reindex();
}

bolt::ErrorCode freelist::write(impl::page *p) {
    p->flags |= impl::freeListPageFlag;

    auto lenids = count();
    if (lenids == 0) {
        p->count = (std::uint16_t)lenids;
    } else if (lenids < 0xFFFF) {
        impl::pgid *ptr = reinterpret_cast<impl::pgid *>(&p->ptr);
        p->count = (std::uint16_t)lenids;
        std::span<impl::pgid> s(ptr, lenids);
        copyall(s);
    } else {
        p->count = 0xFFFF;
        impl::pgid *ptr = reinterpret_cast<impl::pgid *>(&p->ptr);
        ptr[0] = (impl::pgid)lenids;
        std::span<impl::pgid> s(&ptr[1], lenids);
        copyall(s);
    }
    return bolt::ErrorCode::Success;
}

void freelist::reload(impl::page *p) {
    read(p);
    std::map<impl::pgid, bool> pcache;
    for (auto &[_, pendingIDs] : pending) {
        for (auto &pendingID : pendingIDs) {
            pcache[pendingID] = true;
        }
    }
    std::vector<impl::pgid> a;
    for (auto item : ids) {
        auto it = pcache.find(item);
        if (it != pcache.end()) {
            a.push_back(item);
        }
    }
    ids = a;
    reindex();
}

void freelist::reindex() {
    for (auto &item : ids) {
        cache[item] = true;
    }
    for (auto &[_, pendingIDs] : pending) {
        for (auto &pendingID : pendingIDs) {
            cache[pendingID] = true;
        }
    }
}

} // namespace bolt::impl
