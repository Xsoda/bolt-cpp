#pragma once
#include <algorithm>
#include <span>
#include <string>
#include "impl/bsearch.hpp"

template <class ContainerA, class ContainerB>
constexpr bool Equal(ContainerA a, ContainerB b) {
    return std::is_eq(bolt::impl::compare_three_way(a, b));
}

template <class Container>
constexpr std::span<const std::byte> to_bytes(Container &container) {
    return std::span<const std::byte>(reinterpret_cast<const std::byte*>(container.data()), container.size());
}

template <class Container>
std::string to_string(Container &container) {
    return std::string(reinterpret_cast<const char*>(container.data()), container.size());
}
