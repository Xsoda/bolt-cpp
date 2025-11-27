#ifndef __DEFER_HPP__
#define __DEFER_HPP__

#include <functional>

template <typename F> struct Defer {
    F f;
    Defer(F &&f) : f(std::move(f)){};
    ~Defer() { f(); };
};

template <typename F> Defer<F> DeferFunc(F &&f) {
    return Defer<F>(std::move(f));
}


#define DEFER_1(x, y) x##y
#define DEFER_2(x, y) DEFER_1(x, y)
#define DEFER_3(x) DEFER_2(x, __COUNTER__)

#define defer(code) auto DEFER_3(_defer_) = DeferFunc([&](){ code; })

#endif  // !__DEFER_HPP__
