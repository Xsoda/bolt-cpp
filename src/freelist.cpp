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

void mergepgids(std::vector<bolt::pgid> &dest, const std::vector<bolt::pgid> &a, const std::vector<bolt::pgid> &b) {
    if (a.size() == 0) {
        std::copy(b.begin(), b.end(), std::back_inserter(dest));
        return;
    }
    if (b.size() == 0) {
        std::copy(a.begin(), b.end(), std::back_inserter(dest));
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
        std::copy(lead.begin(), it, std::back_inserter(dest));
        if (it == lead.end()) {
            break;
        }
        lead.subspan(std::distance(lead.begin(), it));
        std::swap(lead, follow);
    }
    std::copy(follow.begin(), follow.end(), std::back_inserter(dest));
}

void freelist::copyall(std::vector<bolt::pgid> &dest) {
    std::vector<bolt::pgid> m;
    for (auto it : pending) {
        std::copy(it.second.begin(), it.second.end(), std::back_inserter(m));
    }
    std::sort(m.begin(), m.end(), std::greater<bolt::pgid>());
    mergepgids(dest, ids, m);
}

bolt::pgid freelist::allocate(int n) {
    if (ids.size() == 0) {
        return 0;
    }
    bolt::pgid initial = 0, previd = 0;
    for (auto id : ids) {
        if (id <= 1) {
            assert("invalid page allocation" && 0);
        }
    }
}


}
