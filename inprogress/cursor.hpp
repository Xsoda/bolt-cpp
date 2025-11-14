#ifndef __CURSOR_HPP__
#define __CURSOR_HPP__

#include "common.hpp"

namespace bolt {

class Cursor {
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

}
#endif  // !__CURSOR_HPP__
