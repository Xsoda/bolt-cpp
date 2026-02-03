#include <algorithm>
#include "impl/bsearch.hpp"

template <class A, class B> constexpr bool Equal(A a, B b) {
    return std::is_eq(bolt::impl::compare_three_way(a, b));
}
