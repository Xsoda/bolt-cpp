#include "page.hpp"

namespace bolt {

std::string page::type() const {
    if (flags & branchPageFlag) {
        return "branch";
    } else if (flags & leafPageFlag) {
        return "leaf";
    } else if (flags & metaPageFlag) {
        return "meta";
    } else if (flags & freelistPageFlag) {
        return "freelist";
    } else {
        return "unknown";
    }
}

bolt::meta *page::meta() {
    return reinterpret_cast<bolt::meta*>(&this->ptr);
}

bolt::leafPageElement *page::leafPageElement(std::uint16_t index) {
    bolt::leafPageElement *array = reinterpret_cast<bolt::leafPageElement*>(&this->ptr);
    return &array[index];
}

std::span<bolt::leafPageElement> page::leafPageElements() {
    bolt::leafPageElement *array = reinterpret_cast<bolt::leafPageElement*>(&this->ptr);
    if (this->count == 0) {
        return std::span<bolt::leafPageElement>{};
    }
    return std::span<bolt::leafPageElement>(array, this->count);
}

bolt::branchPageElement *page::branchPageElement(std::uint16_t index) {
    bolt::branchPageElement *array = reinterpret_cast<bolt::branchPageElement*>(&this->ptr);
    return &array[index];
}

std::span<bolt::branchPageElement> page::branchPageElements() {
    bolt::branchPageElement *array = reinterpret_cast<bolt::branchPageElement*>(&this->ptr);
    if (this->count == 0) {
        return std::span<bolt::branchPageElement>{};
    }
    return std::span<bolt::branchPageElement>(array, this->count);
}

bolt::bytes branchPageElement::key() {
    std::byte *buf = reinterpret_cast<std::byte *>(this);
    return bolt::bytes(&buf[this->pos], this->ksize);
}

bolt::bytes leafPageElement::key() {
    std::byte *buf = reinterpret_cast<std::byte *>(this);
    return bolt::bytes(&buf[this->pos], this->ksize);
}

bolt::bytes leafPageElement::value() {
    std::byte *buf = reinterpret_cast<std::byte *>(this);
    return bolt::bytes(&buf[this->pos + this->ksize], this->vsize);
}

}
