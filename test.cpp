#include <iostream>
#include <span>
#include <vector>
#include <algorithm>

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
}
