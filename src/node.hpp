#ifndef __NODE_HPP__
#define __NODE_HPP__

#include "common.hpp"
#include "bucket.hpp"
#include "page.hpp"
#include <initializer_list>

namespace bolt {

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
struct inode {
    std::uint32_t flags;
    bolt::pgid pgid;
    bolt::bytes key;
    bolt::bytes value;
    std::vector<std::byte> memory;

    void set_keyvalue(bolt::bytes key, bolt::bytes value);
};

// node represents an in-memory, deserialized page.
struct node {
    bolt::Bucket *bucket;
    bool isLeaf;
    bool unbalanced;
    bool spilled;
    bolt::bytes key;
    bolt::pgid pgid;
    bolt::node *parent;
    std::vector<bolt::node*> children;
    std::vector<bolt::inode> inodes;
    std::vector<std::byte> memory;

    node(bolt::Bucket *bucket, bool isLeaf, bolt::node *parent);
    node(bolt::Bucket *bucket, std::initializer_list<bolt::node*> children);

    // root returns the top-level node this node is attached to.
    bolt::node *root();

    // minKeys returns the minimum number of inodes this node should have.
    int minKeys() const;

    // size returns the size of the node after serialization.
    int size() const;

    // sizeLessThan returns true if the node is less than a given size.
    // This is an optimization to avoid calculating a large node when we only need
    // to know if it fits inside a certain page size.
    bool sizeLessThan(int v) const;

    // pageElementSize returns the size of each page element based on the type of node.
    int pageElementSize() const;

    // childAt returns the child node at a given index.
    bolt::node *childAt(int index);

    // childIndex returns the index of a given child node.
    int childIndex(const bolt::node *child);

    // numChildren returns the number of children.
    int numChildren() const;

    // nextSibling returns the next node with the same parent.
    node *nextSibling() const;

    // prevSibling returns the previous node with the same parent.
    node *prevSibling() const;

    // put inserts a key/value.
    void put(bolt::bytes oldKey, bolt::bytes newKey, bolt::bytes value, bolt::pgid pgid, std::uint32_t flags);

    // del removes a key from the node.
    void del(bolt::bytes key);

    // read initializes the node from a page.
    void read(bolt::page *p);

    // write writes the items onto one or more pages.
    void write(bolt::page *p);

    // split breaks up a node into multiple smaller nodes, if appropriate.
    // This should only be called from the spill() function.
    std::vector<bolt::node*> split(int pageSize);

    // splitTwo breaks up a node into two smaller nodes, if appropriate.
    // This should only be called from the split() function.
    std::tuple<bolt::node*, bolt::node*> splitTwo(int pageSize);

    // splitIndex finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from split().
    std::tuple<int, int> splitIndex(int threshold);

    // spill writes the nodes to dirty pages and splits nodes as it goes.
    // Returns an error if dirty pages cannot be allocated.
    bolt::ErrorCode spill();

    // rebalance attempts to combine the node with sibling nodes if the node fill
    // size is below a threshold or if there are not enough keys.
    void rebalance();

    // removes a node from the list of in-memory children.
    // This does not affect the inodes.
    void removeChild(bolt::node *target);

    // dereference causes the node to copy all its inode key/value references to heap memory.
    // This is required when the mmap is reallocated so inodes are not pointing to stale data.
    void dereference();

    // free adds the node's underlying page to the freelist.
    void free();
};

}

#endif  // !__NODE_HPP__
