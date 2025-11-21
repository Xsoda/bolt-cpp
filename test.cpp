#include <future>
#include <iostream>
#include <span>
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
using namespace std::chrono_literals;

void print_span(std::span<int> s)
{
    for (auto n : s)
        std::cout << n << ' ';
    std::cout << '\n';
}

int main()
{
    std::vector<int> b{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::span<int> span;
    std::vector<int> copy;

    span = std::span<int>{b};
    print_span(span);

    auto it = std::find_if(span.begin(), span.end(), [&](auto &item) -> bool {
        return item == 5;
    });
    std::copy(span.begin(), it, std::back_inserter(copy));
    print_span(copy);

    span = span.subspan(std::distance(span.begin(), it));
    std::copy(copy.begin(), copy.end(), span.begin());
    print_span(span);

    std::vector<int> vec = {1, 2, 3,4,5,6,7,7,9};

    std::ignore = std::async(std::launch::async, [&]() {
      for (auto &it : vec) {
          std::cout << int(it) << std::endl;
      }
    });
    // f.wait();
}
