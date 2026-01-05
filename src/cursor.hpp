#ifndef __CURSOR_HPP__
#define __CURSOR_HPP__

#include "common.hpp"
#include "error.hpp"
#include <memory>
#include <tuple>

namespace bolt {

struct page;
struct node;

struct elemRef {
    bolt::page *page;
    std::weak_ptr<bolt::node> node;
    int index;

    elemRef(bolt::page *page, bolt::node_ptr node) : page(page), node(node){};
    elemRef(bolt::page *page, bolt::node_ptr node, int index)
        : page(page), node(node), index(index){};
    bool isLeaf() const;
    int count() const;
};

struct Cursor : public std::enable_shared_from_this<Cursor> {
    std::weak_ptr<bolt::Bucket> bucket;
    std::vector<bolt::elemRef> stack;

    explicit Cursor(bolt::BucketPtr bucket): bucket(bucket) {};
    bolt::BucketPtr Bucket();
    std::tuple<bolt::bytes, bolt::bytes> First();
    std::tuple<bolt::bytes, bolt::bytes> Last();
    std::tuple<bolt::bytes, bolt::bytes> Next();
    std::tuple<bolt::bytes, bolt::bytes> Prev();
    std::tuple<bolt::bytes, bolt::bytes> Seek(bolt::bytes seek);
    bolt::ErrorCode Delete();

    std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> seek(bolt::bytes k);
    void first();
    void last();
    std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> next();
    void search(bolt::bytes key, bolt::pgid pgid);
    void nsearch(bolt::bytes key);
    void searchPage(bolt::bytes key, bolt::page *p);
    void searchNode(bolt::bytes key, bolt::node_ptr n);

    std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> keyValue();
    bolt::node_ptr node() const;
};

}
#endif  // !__CURSOR_HPP
