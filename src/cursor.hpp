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

    std::optional<bolt::pair> First();
    std::optional<bolt::pair> Last();
    std::optional<bolt::pair> Next();
    std::optional<bolt::pair> Prev();
    std::optional<bolt::pair> Seek(bolt::bytes seek);

    std::tuple<std::optional<bolt::pair>, std::uint32_t> keyValue();
    bolt::node *node() const;
};

}
#endif  // !__CURSOR_HPP
