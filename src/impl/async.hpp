#ifndef __ASYNC_HPP__
#define __ASYNC_HPP__

#include <functional>
#include <stdexcept>
#include <iostream>
#include <thread>
#include <tuple>
#include "fmt/core.h"

namespace bolt::impl {

    template <typename Fn, typename... Args> void AsyncFireAndForget(Fn &&func, Args &&...args) {
        std::thread([func = std::forward<Fn>(func),
                     args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            try {
                std::apply(std::move(func), std::move(args));
            } catch (std::exception &e) {
                fmt::println(stderr, "Async task failed, {}", e.what());
            } catch (...) {
                fmt::println(stderr, "Async task failed with unknown exception");
            }
        }).detach();
    }

}
#endif  // !__ASYNC_HPP__
