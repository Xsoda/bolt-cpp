#include "page.hpp"
#include "common.hpp"
#include "utils.hpp"

namespace bolt::impl {

std::string page::type() const {
    if (flags & impl::branchPageFlag) {
        return "branch";
    } else if (flags & impl::leafPageFlag) {
        return "leaf";
    } else if (flags & impl::metaPageFlag) {
        return "meta";
    } else if (flags & impl::freeListPageFlag) {
        return "freelist";
    } else {
        return "unknown";
    }
}

impl::meta *page::meta() {
    return reinterpret_cast<impl::meta*>(&this->ptr);
}

impl::leafPageElement *page::leafPageElement(std::uint16_t index) {
    impl::leafPageElement *array = reinterpret_cast<impl::leafPageElement*>(&this->ptr);
    return &array[index];
}

std::span<impl::leafPageElement> page::leafPageElements() {
    impl::leafPageElement *array = reinterpret_cast<impl::leafPageElement*>(&this->ptr);
    if (this->count == 0) {
        return std::span<impl::leafPageElement>{};
    }
    return std::span<impl::leafPageElement>(array, this->count);
}

impl::branchPageElement *page::branchPageElement(std::uint16_t index) {
    impl::branchPageElement *array = reinterpret_cast<impl::branchPageElement*>(&this->ptr);
    return &array[index];
}

std::span<impl::branchPageElement> page::branchPageElements() {
    impl::branchPageElement *array = reinterpret_cast<impl::branchPageElement*>(&this->ptr);
    if (this->count == 0) {
        return std::span<impl::branchPageElement>{};
    }
    return std::span<impl::branchPageElement>(array, this->count);
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
