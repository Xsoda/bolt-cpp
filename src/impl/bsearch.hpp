#pragma once
#include <algorithm>
#include <iterator>

#ifndef __BSEARCH_HPP__
#define __BSEARCH_HPP__

namespace bolt::impl {

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
            first = std::next(it);
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
            first = std::next(it);
            count -= step + 1;
        } else
            count = step;
    }

    return first;
}

template <class ContainerA, class ContainerB>
constexpr auto compare_three_way(ContainerA a, ContainerB b) {
    return std::lexicographical_compare_three_way(std::begin(a), std::end(a),
                                                  std::begin(b), std::end(b));
}

}
#endif  // !__BSEARCH_HPP__
