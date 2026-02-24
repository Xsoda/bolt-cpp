#pragma once

#ifndef __PIMPL_HPP__
#define __PIMPL_HPP__

#include <memory>

namespace bolt {

template <typename T> class pimpl {
public:
    template <typename... Args>
    pimpl(Args... args)
        : pImpl(std::make_unique<T>(std::forward<Args>(args)...)) {};
    pimpl() : pImpl(std::make_unique<T>()){};
    pimpl(std::unique_ptr<T> uptr) : pImpl(std::move(uptr)){};
    T *operator->() { return pImpl.get(); };
    T &operator*() { return *pImpl.get(); };
    virtual ~pimpl() = default;
    pimpl &operator=(pimpl<T> &&p) {
        pImpl = std::move(p.pImpl);
        return *this;
    };
    pimpl &operator=(const pimpl<T> &) = delete;
    operator bool() { return pImpl != nullptr; };
protected:
    T &impl() { return *pImpl; };
private:
    std::unique_ptr<T> pImpl;
};

} // namespace bolt

#endif  // !__PIMPL_HPP__
