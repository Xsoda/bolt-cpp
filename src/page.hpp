#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include "common.hpp"

namespace bolt {

struct meta;

struct branchPageElement {
    std::uint32_t pos;
    std::uint32_t ksize;
    bolt::pgid pgid;

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
    bolt::pgid id;
    std::uint16_t flags;
    std::uint16_t count;
    std::uint32_t overflow;
    std::uintptr_t ptr;

    std::string type() const;
    bolt::meta *meta();
    bolt::leafPageElement *leafPageElement(std::uint16_t index);
    std::span<bolt::leafPageElement> leafPageElements();

    bolt::branchPageElement *branchPageElement(std::uint16_t index);
    std::span<bolt::branchPageElement> branchPageElements();
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
