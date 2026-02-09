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
    std::uint32_t RandomInt(std::uint32_t min, std::uint32_t max) {
        auto length = Random();
        length %= (max - min);
        length += min;
        return (uint32_t)length;
    };
    void Generate(size_t size) {
        items.reserve(size);
        std::vector<std::uint32_t> lens;
        lens.reserve(size * 2);
        for (size_t i = 0; i < size; i++) {
            lens.push_back(RandomInt(1, 1024));
            lens.push_back(RandomInt(0, 1024));
        }
        std::uint32_t total = 0;
        for (auto it : lens) {
            total += it;
        }
        memory.resize(total);
        RandomCharset(memory);
        std::uint32_t offset = 0;
        for (size_t i = 0; i < size; i++) {
            auto k = std::span<std::byte>(reinterpret_cast<std::byte*>(memory.data() + offset), lens[i * 2]);
            offset += lens[i * 2];
            auto v = std::span<std::byte>(reinterpret_cast<std::byte*>(memory.data() + offset), lens[i * 2 + 1]);
            offset += lens[i * 2 + 1];
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
