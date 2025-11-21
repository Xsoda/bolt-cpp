#ifndef __CURSOR_HPP__
#define __CURSOR_HPP__

#include "common.hpp"
#include <tuple>

namespace bolt {

struct Bucket;
struct page;
struct node;

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

    bolt::tuple<bolt::bytes, bolt::bytes> First();
    bolt::tuple<bolt::bytes, bolt::bytes> Last();
    bolt::tuple<bolt::bytes, bolt::bytes> Next();
    bolt::tuple<bolt::bytes, bolt::bytes> Prev();
    bolt::tuple<bolt::bytes, bolt::bytes> Seek(bolt::bytes seek);

    std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> keyValue();
    bolt::node *node() const;
};

}
#endif  // !__CURSOR_HPP
