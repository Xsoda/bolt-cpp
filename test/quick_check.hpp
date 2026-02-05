#pragma once

#include <span>
#include <vector>
#include <algorithm>
#include <cstdint>
#include "impl/bsearch.hpp"
#include "random.hpp"

struct TestData {
    using Item =
        std::pair<std::span<const std::byte>, std::span<const std::byte>>;
    std::vector<std::byte> memory;
    std::vector<Item> items;
    std::span<std::byte> RandomBytes(std::uint32_t min, std::uint32_t max) {
        auto length = Random();
        length %= (max - min);
        length += min;
        memory.resize(memory.size() + length);
        std::span<std::byte> result =
            std::span<std::byte>(reinterpret_cast<std::byte *>(
                                     memory.data() + memory.size() - length),
                                 length);
        return RandomCharset(result);
    };
    void Generate(size_t size) {
        items.reserve(size);
        for (size_t i = 0; i < size; i++) {
            auto k = RandomBytes(1, 1024);
            auto v = RandomBytes(0, 1024);
            items.push_back(std::make_pair(k, v));
        }
    };
    void Sort() {
        std::sort(items.begin(), items.end(),
                  [](const Item &a, const Item &b) -> bool {
                      return std::is_lt(bolt::impl::compare_three_way(a.first, b.first));
                  });
    };
    auto begin() { return items.begin(); };
    auto end() { return items.end(); };
    auto &operator[](size_t index) noexcept { return items[index]; };
    auto size() { return items.size(); };
};

class QuickCheck {
  public:
    bool Check(std::function<bool(TestData &testdata)> fn, size_t size = 1000) {
        testdata.Generate(size);
        return fn(testdata);
    };

  private:
    TestData testdata;
};
