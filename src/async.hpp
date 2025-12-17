#ifndef __ASYNC_HPP__
#define __ASYNC_HPP__

#include <functional>
#include <stdexcept>
#include <iostream>
#include <thread>
#include <tuple>

namespace bolt {

template <typename Fn, typename... Args> void AsyncFireAndForget(Fn &&func, Args &&...args) {
  std::thread([func = std::forward<Fn>(func),
               args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
    try {
        std::apply(std::move(func), std::move(args));
    } catch (std::exception &e) {
        std::cerr << "Async task failed, " << e.what() << "\n";
    } catch (...) {
        std::cerr << "Async task failed with unknown exception" << "\n";
    }
  }).detach();
}

}
#endif  // !__ASYNC_HPP__
