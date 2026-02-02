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

template <class ForwardIt,
          class T = typename std::iterator_traits<ForwardIt>::value_type,
          class Compare>
std::tuple<ForwardIt, std::strong_ordering>
bsearch2(ForwardIt first, ForwardIt last, const T &value, Compare comp) {
    ForwardIt it = first;
    auto ret = std::strong_ordering::less;
    typename std::iterator_traits<ForwardIt>::difference_type count, step;
    count = std::distance(first, last);

    while (count > 0) {
        it = first;
        step = count / 2;
        ret = comp(value, *it);
        if (std::is_lt(ret)) {
            count = step;
        } else if (std::is_gt(ret)) {
            first = ++it;
            count -= step + 1;
        } else {
            break;
        }
    }
    return std::make_tuple(first, ret);
}

template <class ForwardIt,
          class T = typename std::iterator_traits<ForwardIt>::value_type,
          class Compare>
ForwardIt upper_bound(ForwardIt first, ForwardIt last, const T &value,
                      Compare comp) {
    ForwardIt it;
    typename std::iterator_traits<ForwardIt>::difference_type count, step;
    count = std::distance(first, last);

    while (count > 0) {
        it = first;
        step = count / 2;
        std::advance(it, step);

        if (!comp(value, *it)) {
            first = ++it;
            count -= step + 1;
        } else
            count = step;
    }

    return first;
}

template <class ForwardIt,
          class T = typename std::iterator_traits<ForwardIt>::value_type,
          class Compare>
ForwardIt lower_bound(ForwardIt first, ForwardIt last, const T &value,
                      Compare comp) {
    ForwardIt it;
    typename std::iterator_traits<ForwardIt>::difference_type count, step;
    count = std::distance(first, last);

    while (count > 0) {
        it = first;
        step = count / 2;
        std::advance(it, step);

        if (comp(*it, value)) {
            first = ++it;
            count -= step + 1;
        } else
            count = step;
    }

    return first;
}
}
#endif  // !__BSEARCH_HPP__
