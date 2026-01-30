#pragma once
#include <compare>
#ifndef __BSEARCH_HPP__
#define __BSEARCH_HPP__

#include <iterator>
#include <tuple>

namespace bolt::impl {

template <class ForwardIt,
          class T = typename std::iterator_traits<ForwardIt>::value_type,
          class Compare>
std::tuple<ForwardIt, std::strong_ordering> bsearch(ForwardIt first, ForwardIt last,
                                                    const T &value, Compare comp) {
    ForwardIt it = first;
    auto ret = std::strong_ordering::equal;
    typename std::iterator_traits<ForwardIt>::difference_type left, right,
        middle;
    left = 0;
    right = std::distance(first, last) - 1;
    while (left <= right) {
        middle = left + ((right - left) >> 1);
        it = std::next(first, middle);
        ret = comp(value, *it);
        if (std::is_lt(ret)) {
            right = middle - 1;
        } else if (std::is_gt(ret)) {
            left = middle + 1;
        } else {
            break;
        }
    }
    if (!std::is_eq(ret)) {
        it = std::next(first, left);
    }
    return std::make_tuple(it, ret);
}
}
#endif  // !__BSEARCH_HPP__
