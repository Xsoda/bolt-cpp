#pragma once

#ifndef __PIMPL_HPP__
#define __PIMPL_HPP__

namespace bolt {
#include <memory>

template <typename T> class pimpl {
public:
    template <typename... Args>
    pimpl(Args... args)
        : pImpl(std::make_unique<T>(std::forward<Args>(args)...)) {};
    pimpl() : pImpl(std::make_unique<T>()){};
    pimpl(std::unique_ptr<T> oth) : pImpl(std::move(oth)){};
    T *operator->() { return pImpl.get(); };
    T &operator*() { return *pImpl.get(); };
    virtual ~pimpl() = default;
    pimpl &operator=(pimpl<T> &&p) {
        pImpl = std::move(p.pImpl);
        return *this;
    };
    pimpl &operator=(const pimpl<T> &) = delete;

private:
    std::unique_ptr<T> pImpl;
};
}

#endif  // !__PIMPL_HPP__
