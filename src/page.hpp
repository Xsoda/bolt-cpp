#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include "common.hpp"
#include "utils.hpp"

namespace bolt::impl {

struct meta;

struct branchPageElement {
    std::uint32_t pos;
    std::uint32_t ksize;
    impl::pgid pgid;

    bolt::bytes key();
};

struct leafPageElement {
    std::uint32_t flags;
    std::uint32_t pos;
    std::uint32_t ksize;
    std::uint32_t vsize;

    bolt::bytes key();
    bolt::bytes value();
};

struct page {
    impl::pgid id;
    std::uint16_t flags;
    std::uint16_t count;
    std::uint32_t overflow;
    std::uintptr_t ptr;

    page() = default;
    page(impl::pgid id): id(id), flags(0), count(0), overflow(0) {};
    page(impl::pgid id, std::uint32_t overflow): id(id), overflow(overflow), flags(0), count(0) {};
    std::string type() const;
    impl::meta *meta();
    impl::leafPageElement *leafPageElement(std::uint16_t index);
    std::span<impl::leafPageElement> leafPageElements();

    impl::branchPageElement *branchPageElement(std::uint16_t index);
    std::span<impl::branchPageElement> branchPageElements();
};


struct PageInfo {
    int ID;
    std::string Type;
    int Count;
    int OverflowCount;
};

const int branchPageElementSize = sizeof(branchPageElement);
const int leafPageElementSize = sizeof(leafPageElement);
const int pageHeaderSize = offsetof(page, ptr);

}

#endif  // !__PAGE_HPP__
