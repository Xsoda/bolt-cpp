#pragma once

#include <cstdint>
#include <string>
#include <span>
#include <algorithm>

std::uint64_t Random();
std::string RandomCharset(size_t length);

template <class Container> Container RandomCharset(Container &output) {
    static std::string charset =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    auto length = output.size();
    for (size_t i = 0; i < length; i++) {
        std::uint64_t index = Random() % charset.size();
        output[i] = (decltype(output[i]))(charset[index]);
    }
    return output;
}
