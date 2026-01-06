#ifndef __FILE_HPP__
#define __FILE_HPP__

#include "bolt/pimpl.hpp"
#include "utils.hpp"
#include <tuple>
#include <chrono>

namespace bolt::impl {

    int Getpagesize();

    struct FileImpl;
    struct File {
        explicit File();
        ~File();
        std::tuple<std::uint64_t, bolt::ErrorCode> WriteAt(bolt::bytes buf,
                                                           std::uint64_t offset);
        std::tuple<std::uint64_t, bolt::ErrorCode> ReadAt(bolt::bytes buf,
                                                          std::uint64_t offset);
        bolt::ErrorCode Fdatasync();
        bolt::ErrorCode Fsync();
        bolt::ErrorCode Flock(bool exclusive, std::chrono::milliseconds timeout);
        bolt::ErrorCode Funlock();
        bolt::ErrorCode Open(std::string path, bool readOnly);
        bolt::ErrorCode Close();
        bolt::ErrorCode Truncate(std::uint64_t size);
        std::tuple<std::uint64_t, bolt::ErrorCode> Size();
        std::tuple<std::uintptr_t, bolt::ErrorCode> Mmap(std::uint64_t size);
        bolt::ErrorCode Munmap(std::uintptr_t ptr);

    private:
        bolt::pimpl<impl::FileImpl> pImpl;
    };
}
#endif  // __FILE_HPP__
