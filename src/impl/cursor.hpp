#ifndef __CURSOR_HPP__
#define __CURSOR_HPP__

#include "utils.hpp"
#include <memory>
#include <tuple>

namespace bolt::impl {

struct page;
struct node;

struct elemRef {
    impl::page *page;
    std::weak_ptr<impl::node> node;
    int index;

    elemRef(impl::page *page, impl::node_ptr node) : page(page), node(node){};
    elemRef(impl::page *page, impl::node_ptr node, int index)
        : page(page), node(node), index(index){};
    bool isLeaf() const;
    int count() const;
};

struct Cursor : public std::enable_shared_from_this<Cursor> {
    std::weak_ptr<impl::Bucket> bucket;
    std::vector<impl::elemRef> stack;

    explicit Cursor(impl::BucketPtr bucket): bucket(bucket) {};
    impl::BucketPtr Bucket();
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
    void search(bolt::bytes key, impl::pgid pgid);
    void nsearch(bolt::bytes key);
    void searchPage(bolt::bytes key, impl::page *p);
    void searchNode(bolt::bytes key, impl::node_ptr n);

    std::tuple<bolt::bytes, bolt::bytes, std::uint32_t> keyValue();
    impl::node_ptr node() const;
};

}
#endif  // !__CURSOR_HPP
