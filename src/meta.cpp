#include "meta.hpp"
#include "page.hpp"
#include "fnv64.hpp"
#include <cassert>

namespace bolt {

void meta::copy(bolt::meta *dest) const {
    *dest = *this;
}

void meta::write(bolt::page *p) {
    assert(this->root.root >= pgid);
    assert(this->freelist > pgid);

    p->id = this->txid % 2;
    p->flags |= bolt::metaPageFlag;

    checksum = this->sum64();

    copy(p->meta());
}

std::uint64_t meta::sum64() {
    uint8_t *buf = reinterpret_cast<uint8_t *>(this);
    size_t len = offsetof(meta, checksum);

    return fnv64::fnv1a_hash(buf, len);
}

int meta::validate() {
    if (this->magic != bolt::magic) {
        return -1;
    } else if (this->version != bolt::version) {
        return -1;
    } else if (this->checksum != 0 && this->checksum != this->sum64()) {
        return -1;
    }
    return 0;
}

}
