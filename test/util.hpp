#include <algorithm>

template <typename A, typename B> bool Compare(A a, B b) {
    return std::is_eq(std::lexicographical_compare_three_way(a.begin(), a.end(),
                                                           b.begin(), b.end()));
}
