#include "random.hpp"

static std::string charset =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

std::uint64_t Random() {
    static std::uint64_t seed = 13;
    seed = seed * 997 + 521;
    return seed;
}

std::string RandomCharset(size_t length) {
    std::string value;
    value.reserve(length);
    for (size_t i = 0; i < length; i++) {
        std::uint64_t index = Random() % charset.size();
        value.push_back(charset[index]);
    }
    return std::move(value);
}

void RandomCharset(std::string &output) {
    auto length = output.size();
    for (size_t i = 0; i < length; i++) {
        std::uint64_t index = Random() % charset.size();
        output[i] = charset[index];
    }
}
