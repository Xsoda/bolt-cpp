#include "meta.hpp"
#include "common.hpp"
#include "page.hpp"
#include "fnv64.hpp"
#include <cassert>

namespace bolt {

meta::meta() {
    magic = bolt::magic;
    version = bolt::version;
    pageSize = bolt::defaultPageSize;
    flags = 0;
    root.root = 0;
    root.sequence = 0;
    freelist = 0;
    pgid = 0;
    txid = 0;
    checksum = 0;
}

meta::meta(bolt::pgid id) {
    magic = bolt::magic;
    version = bolt::version;
    pageSize = bolt::defaultPageSize;
    flags = 0;
    root.root = 0;
    root.sequence = 0;
    freelist = 0;
    pgid = id;
    txid = 0;
    checksum = 0;
}

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

bolt::ErrorCode meta::validate() {
    if (this->magic != bolt::magic) {
        return bolt::ErrorCode::ErrorDatabaseInvalid;
    } else if (this->version != bolt::version) {
        return bolt::ErrorCode::ErrorVersionMismatch;
    } else if (this->checksum != 0 && this->checksum != this->sum64()) {
        return bolt::ErrorCode::ErrorChecksum;
    }
    return bolt::ErrorCode::Success;
}

}
