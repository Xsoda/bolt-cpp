#ifndef __NODE_HPP__
#define __NODE_HPP__

#include "impl/utils.hpp"
#include "impl/bucket.hpp"
#include "impl/page.hpp"
#include <initializer_list>
#include <memory>

namespace bolt::impl {

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
struct inode {
    std::uint32_t flags;
    impl::pgid pgid;
    bolt::bytes key;
    bolt::bytes value;
    std::vector<std::byte> memory;
};

// node represents an in-memory, deserialized page.
struct node : public std::enable_shared_from_this<node> {
    std::weak_ptr<impl::Bucket> bucket;
    bool isLeaf;
    bool unbalanced;
    bool spilled;
    bolt::bytes key;
    impl::pgid pgid;
    std::weak_ptr<impl::node> parent;
    std::vector<impl::node_ptr> children;
    std::vector<impl::inode> inodes;
    std::vector<std::byte> memory;

    explicit node(){};
    explicit node(bool isLeaf);
    explicit node(impl::BucketPtr bucket);
    explicit node(impl::BucketPtr bucket, bool isLeaf, impl::node_ptr parent);
    explicit node(impl::BucketPtr bucket, std::initializer_list<impl::node_ptr> children);

    // root returns the top-level node this node is attached to.
    impl::node_ptr root();

    // minKeys returns the minimum number of inodes this node should have.
    size_t minKeys() const;

    // size returns the size of the node after serialization.
    size_t size() const;

    // sizeLessThan returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    bool sizeLessThan(size_t v) const;

    // pageElementSize returns the size of each page element based on the type of node.
    size_t pageElementSize() const;

    // childAt returns the child node at a given index.
    impl::node_ptr childAt(ptrdiff_t index);

    // childIndex returns the index of a given child node.
    ptrdiff_t childIndex(impl::node_ptr child);

    // numChildren returns the number of children.
    size_t numChildren() const;

    // nextSibling returns the next node with the same parent.
    impl::node_ptr nextSibling();

    // prevSibling returns the previous node with the same parent.
    impl::node_ptr prevSibling();

    // put inserts a key/value.
    void put(bolt::bytes oldKey, bolt::bytes newKey, bolt::bytes value, impl::pgid pgid, std::uint32_t flags);

    // del removes a key from the node.
    void del(bolt::bytes key);

    // read initializes the node from a page.
    void read(impl::page *p);

    // write writes the items onto one or more pages.
    void write(impl::page *p);

    // split breaks up a node into multiple smaller nodes, if appropriate.
    // This should only be called from the spill() function.
    std::tuple<std::vector<impl::node_ptr>, impl::node_ptr> split(size_t pageSize);

    // splitTwo breaks up a node into two smaller nodes, if appropriate.
    // This should only be called from the split() function.
    // extra return parent pointer used hook std::weak_ptr
    std::tuple<impl::node_ptr, impl::node_ptr, impl::node_ptr> splitTwo(size_t pageSize);

    // splitIndex finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from split().
    std::tuple<size_t, size_t> splitIndex(size_t threshold);

    // spill writes the nodes to dirty pages and splits nodes as it goes.
    // Returns an error if dirty pages cannot be allocated.
    bolt::ErrorCode spill();

    // rebalance attempts to combine the node with sibling nodes if the node fill
    // size is below a threshold or if there are not enough keys.
    void rebalance();

    // removes a node from the list of in-memory children.
    // This does not affect the inodes.
    void removeChild(impl::node_ptr target);

    // dereference causes the node to copy all its inode key/value references to heap memory.
    // This is required when the mmap is reallocated so inodes are not pointing to stale data.
    void dereference();

    // free adds the node's underlying page to the freelist.
    void free();

    void dump();
};

}

#endif  // !__NODE_HPP__
