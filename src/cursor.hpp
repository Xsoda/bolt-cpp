#ifndef __CURSOR_HPP__
#define __CURSOR_HPP__

#include "common.hpp"
#include <optional>

namespace bolt {

struct Bucket;
struct page;
struct node;

using pair = std::pair<bolt::bytes, bolt::bytes>;

struct elemRef {
    bolt::page *page;
    bolt::node *node;
    int index;

    bool isLeaf() const;
    int count() const;
};

struct Cursor {
    bolt::Bucket *bucket;
    std::vector<bolt::elemRef> stack;

    bolt::pair First();
    bolt::pair Last();
    bolt::pair Next();
    bolt::pair Prev();
    bolt::pair Seek(bolt::bytes seek);

    std::tuple<bolt::pair, std::uint32_t> keyValue();
    bolt::node *node() const;
};

}
#endif  // !__CURSOR_HPP
