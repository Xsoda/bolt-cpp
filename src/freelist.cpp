#include "freelist.hpp"
#include "page.hpp"
#include <algorithm>
#include <cassert>

namespace bolt {

int freelist::size() {
    int n = this->count();
    if (n >= 0xFFFF) {
        n++;
    }
    return bolt::pageHeaderSize + int(sizeof(bolt::pgid) * n);
}

int freelist::count() {
    return this->free_count() + this->pending_count();
}

int freelist::free_count() {
    return this->ids.size();
}

int freelist::pending_count() {
    int count = 0;
    for (auto it : pending) {
        count += it.second.size();
    }
    return count;
}

void mergepgids(std::span<bolt::pgid> dest, const std::vector<bolt::pgid> &a, const std::vector<bolt::pgid> &b) {
    if (dest.size() < a.size() + b.size()) {
        assert("mergepgids bad length" && false);
    }
    size_t length = 0;
    if (a.size() == 0) {
        std::copy(b.begin(), b.end(), dest.begin());
        return;
    }
    if (b.size() == 0) {
        std::copy(a.begin(), b.end(), dest.begin());
        return;
    }

    std::span<bolt::pgid> lead{a};
    std::span<bolt::pgid> follow{b};

    if (b[0] < a[0]) {
        std::swap(lead, follow);
    }

    while (lead.size() > 0) {
        auto it = std::find_if(lead.begin(), lead.end(), [&](bolt::pgid &item) -> bool {
            return item > follow[0];
        });
        std::copy(lead.begin(), it, dest.begin() + length);
        length += std::distance(lead.begin(), it);

        if (it == lead.end()) {
            break;
        }
        lead.subspan(std::distance(lead.begin(), it));
        std::swap(lead, follow);
    }
    std::copy(follow.begin(), follow.end(), dest.begin() + length);
}

void freelist::copyall(std::span<bolt::pgid> dest) {
    std::vector<bolt::pgid> m;
    for (auto it : pending) {
        std::copy(it.second.begin(), it.second.end(), std::back_inserter(m));
    }
    std::sort(m);
    mergepgids(dest, ids, m);
}

bolt::pgid freelist::allocate(int n) {
    if (ids.size() == 0) {
        return 0;
    }
    bolt::pgid initial = 0, previd = 0;
    for (int i = 0; i < ids.size(); i++) {
        bolt::pgid id = ids[i];
        if (id <= 1) {
            assert("invalid page allocation" && 0);
        }
        if (previd == 0 || id - previd != 1){
            initial = id;
        }

        if (id - initial + 1 == (bolt::pgid)n) {
            if (i + 1 == n) {
                ids.erase(ids.begin(), ids.begin() + i + 1);
            } else {
                ids.erase(ids.begin() + i + 1, ids.begin() + i + n + 1);
            }

            for (bolt::pgid j = 0; j < (bolt::pgid)n; j++) {
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

void freelist::free(bolt::txid txid, page *p) {
    if (p->id <= 1) {
        assert("cannot free page 0 or 1" && true);
    }

    auto it = pending.find(txid);
    assert(it != pending.end());
    for (bolt::pgid id = p->id; id < p->id + p->overflow; id++) {
        auto ch = cache.find(id);
        assert("page already freed" && ch != cache.end());
        it->second.push_back(id);
        cache[id] = true;
    }
}

void freelist::release(bolt::txid txid) {
    std::vector<bolt::pgid> m, merge;
    for (auto &[key, value] : pending) {
        if (key <= txid) {
            std::copy(value.begin(), value.end(), std::back_inserter(m));
        }
    }
    std::erase_if(pending, [](const auto &item) {
        const auto &[key, value] = item;
        return key <= txid;
    });
    std::sort(m.begin(), m.end());
    merge.assign(m.size() + ids.size(), 0);
    mergepgids(std::span<bolt::pgid>(merge), ids, m);
    ids = merge;
}

void freelist::rollback(bolt::txid txid) {
    auto it = pending.find(txid);
    if (it != pending.end()) {
        std::erase_if(cache, [](const auto &item) {
            const auto &[key, value] = item;
            return std::ranges::contains(it->second, key);
        });
        pending.erase(it);
    }
}

bool freelist::freed(bolt::pgid pgid) {
    auto it = cache.find(pgid);
    if (it != cache.end()) {
        return it->second;
    }
    return false;
}

void freelist::read(bolt::page *p) {
    int idx = 0;
    int count = (int)p->count;
    if (count == 0xFFFF) {
        idx = 1;
        count = (int)reinterpret_cast<bolt::pgid*>(&p->ptr)[0];
    }

    ids.clear();
    if (count > 0) {
        bolt::pgid *ptr = reinterpret_cast<bolt::pgid*>(&p->ptr);
        std::span<bolt::pgid> s(&ptr[idx], count);
        ids.assign(count, 0);
        std::copy(s.begin(), s.end(), ids.begin());
        std::sort(ids);
    }
    reindex();
}

int freelist::write(bolt::page *p) {
    p->flags |= bolt::freeListPageFlag;

    auto lenids = count();
    if (lenids == 0) {
        p->count = (std::uint16_t)lenids;
    } else if (lenids < 0xFFFF) {
        bolt::pgid *ptr = reinterpret_cast<bolt::pgid*>(&p->ptr);
        p->count = (std::uint16_t)lenids;
        std::span<bolt::pgid> s(ptr, lenids);
        copyall(s);
    } else {
        p->count = 0xFFFF;
        bolt::pgid *ptr = reinterpret_cast<bolt::pgid*>(&p->ptr);
        ptr[0] = (bolt::pgid)lenids;
        std::span<bolt::pgid> s(&ptr[1], lenids);
        copyall(s);
    }
    return 0;
}

void freelist::reload(bolt::page *p) {
    read(p);
    std::map<bolt::pgid, bool> pcache;
    for (auto &[_, pendingIDs] : pending) {
        for (auto &pendingID : pendingIDs) {
            pcache[pendingID] = true;
        }
    }
    std::vector<bolt::pgid> a;
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

}
