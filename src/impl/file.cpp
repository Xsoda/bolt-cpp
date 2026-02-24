#include "impl/file.hpp"
#include "bolt/error.hpp"
#include "impl/utils.hpp"
#include <chrono>
#include <climits>
#include <thread>
#include <iostream>

#ifdef WIN32
#include <windows.h>
namespace bolt::impl {

int Getpagesize() {
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    return (int)info.dwPageSize;
}

struct FileImpl {
    FileImpl() : file(INVALID_HANDLE_VALUE), lockfile(INVALID_HANDLE_VALUE), ptr(NULL) {};
    ~FileImpl();
    std::tuple<std::uint64_t, bolt::ErrorCode> WriteAt(bolt::bytes buf,
                                                       std::uint64_t offset);
    std::tuple<std::uint64_t, bolt::ErrorCode>
    WriteAt(std::vector<bolt::bytes> &&bufs, std::uint64_t offset);
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
    std::wstring path;
    std::uintptr_t ptr;
    HANDLE file;
    HANDLE lockfile;
};

std::string wstr2str(const std::wstring &ws) {
    int len;
    int slen = (int)ws.length() + 1;
    len = WideCharToMultiByte(CP_ACP, 0, ws.c_str(), slen, NULL, 0, NULL, NULL);
    std::vector<char> buf;
    buf.assign(len, char(0));
    WideCharToMultiByte(CP_ACP, 0, ws.c_str(), slen, buf.data(), len, NULL, NULL);
    return std::string(buf.data());
}

std::wstring str2wstr(const std::string &s) {
    int len;
    int slength = (int)s.length() + 1;
    len = MultiByteToWideChar(CP_ACP, 0, s.c_str(), slength, NULL, 0);
    std::vector<wchar_t> buf;
    buf.assign(len, wchar_t(0));
    MultiByteToWideChar(CP_ACP, 0, s.c_str(), slength, buf.data(), len);
    return std::wstring(buf.data());
}

FileImpl::~FileImpl() {
    if (ptr != (std::uintptr_t)nullptr) {
        Munmap(ptr);
    }
    if (file != INVALID_HANDLE_VALUE) {
        Close();
    }
    if (lockfile != INVALID_HANDLE_VALUE) {
        Funlock();
    }
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::WriteAt(bolt::bytes buf, std::uint64_t offset) {
    DWORD written;
    LARGE_INTEGER li = {0};
    li.QuadPart = offset;
    if (!SetFilePointerEx(file, li, NULL, FILE_BEGIN)) {
        return std::make_tuple(0, bolt::ErrorSystemCall);
    }
    if (!WriteFile(file, buf.data(), (DWORD)buf.size(), &written, NULL)) {
        return std::make_tuple(0, bolt::ErrorSystemCall);
    }
    if (written != buf.size()) {
        return std::make_tuple(std::uint64_t(written), bolt::Success);
    }
    return std::make_tuple(std::uint64_t(written), bolt::Success);
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::WriteAt(std::vector<bolt::bytes> &&bufs, std::uint64_t offset) {
    std::uint64_t total = 0;
    for (auto it : bufs) {
        std::uint64_t written, w;
        bolt::ErrorCode err;
        written = 0;
        w = 0;
        while (written < it.size()) {
          std::tie(w, err) =
              WriteAt(bolt::bytes{it.data() + written, it.size() - written}, offset + total + written);
            written += w;
            if (err != bolt::Success) {
                break;
            }
        }
        total += written;
        if (err != bolt::Success) {
            return std::make_tuple(std::uint64_t(total), err);
        }
    }
    return std::make_tuple(std::uint64_t(total), bolt::Success);
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::ReadAt(bolt::bytes buf, std::uint64_t offset) {
    DWORD readed;
    LARGE_INTEGER li = {0};
    li.QuadPart = offset;
    if (!SetFilePointerEx(file, li, NULL, FILE_BEGIN)) {
        return std::make_tuple(0, bolt::ErrorSystemCall);
    }
    if (!ReadFile(file, buf.data(), (DWORD)buf.size(), &readed, NULL)) {
        return std::make_tuple(0, bolt::ErrorSystemCall);
    }
    if (readed < buf.size()) {
        return std::make_tuple(std::uint64_t(readed), bolt::ErrorSystemCall);
    }
    return std::make_tuple(std::uint64_t(readed), bolt::Success);
}

bolt::ErrorCode FileImpl::Truncate(std::uint64_t size) {
    LARGE_INTEGER li;
    li.QuadPart = size;
    if (!SetFilePointerEx(file, li, NULL, FILE_BEGIN)) {
        return bolt::ErrorSystemCall;
    }
    if (!SetEndOfFile(file)) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

std::tuple<std::uint64_t, bolt::ErrorCode> FileImpl::Size() {
    LARGE_INTEGER li;
    if (GetFileSizeEx(file, &li)) {
        return std::make_tuple(std::uint64_t(li.QuadPart), bolt::Success);
    }
    return std::make_tuple(std::uint64_t(0), bolt::ErrorSystemCall);
}


bolt::ErrorCode FileImpl::Open(std::string path, bool readOnly) {
    this->path = str2wstr(path);
    SECURITY_ATTRIBUTES sa = { 0 };
    sa.nLength = sizeof(SECURITY_ATTRIBUTES);
    sa.bInheritHandle = true;
    DWORD shareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD dwDesiredAccess = GENERIC_READ;
    if (!readOnly) {
        dwDesiredAccess |= GENERIC_WRITE;
    }
    file = CreateFile(this->path.c_str(), dwDesiredAccess, shareMode, &sa, OPEN_ALWAYS,
                      FILE_ATTRIBUTE_NORMAL, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Close() {
    BOOL ret = CloseHandle(file);
    if (ret) {
        file = INVALID_HANDLE_VALUE;
        return bolt::Success;
    }
    return bolt::ErrorSystemCall;
}

bolt::ErrorCode FileImpl::Flock(bool exclusive,
                                std::chrono::milliseconds timeout) {
    DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
    if (exclusive) {
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    std::wstring lock_path = path;
    lock_path.append(L".lock");
    SECURITY_ATTRIBUTES sa = { 0 };
    sa.nLength = sizeof(SECURITY_ATTRIBUTES);
    sa.bInheritHandle = true;
    HANDLE l = CreateFile(lock_path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_READ,
                          &sa, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (l == INVALID_HANDLE_VALUE) {
        return bolt::ErrorSystemCall;
    }

    lockfile = l;
    auto start =
        std::chrono::system_clock::now();
    while (true) {
        OVERLAPPED sOverlapped = { 0 };
        auto since = std::chrono::system_clock::now();
        if (timeout > 1us &&
            std::chrono::duration_cast<std::chrono::milliseconds>(since - start) >
            timeout) {
            return bolt::ErrorTimeout;
        }
        BOOL ret = LockFileEx(lockfile, flags, 0, 1, 0, &sOverlapped);
        if (ret) {
            break;
        }
        else {
            DWORD err = GetLastError();
            if (err != ERROR_LOCK_VIOLATION) {
                return bolt::ErrorSystemCall;
            }
        }
        std::this_thread::sleep_for(50ms);
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Funlock() {
    OVERLAPPED sOverlapped = { 0 };
    std::wstring lock_path = path;
    lock_path += L".lock";

    BOOL ret = UnlockFileEx(lockfile, 0, 1, 0, &sOverlapped);
    if (!ret) {
        return bolt::ErrorSystemCall;
    }
    CloseHandle(lockfile);
    lockfile = INVALID_HANDLE_VALUE;
    ret = DeleteFile(lock_path.c_str());
    if (!ret) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Fdatasync() {
    int ret = FlushFileBuffers(file) ? 0 : -1;
    if (ret == -1) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Fsync() {
    return Fdatasync();
}

std::tuple<std::uintptr_t, bolt::ErrorCode> FileImpl::Mmap(std::uint64_t size) {
    SECURITY_ATTRIBUTES sa = { 0 };
    sa.nLength = sizeof(SECURITY_ATTRIBUTES);
    sa.bInheritHandle = true;
    DWORD sizehi = (DWORD)(size >> 32);
    DWORD sizelo = (DWORD)(size & 0xFFFFFFFF);
    _assert(ptr == NULL, "mmap ptr is nullptr");
    HANDLE fm =
        CreateFileMapping(file, &sa, PAGE_READONLY, sizehi, sizelo, NULL);
    if (fm == NULL) {
        auto [size, err] = Size();
        return std::make_tuple((std::uintptr_t)nullptr,
                               bolt::ErrorSystemCall);
    }
    LPVOID view = MapViewOfFile(fm, FILE_MAP_READ, 0, 0, size);
    CloseHandle(fm);
    if (view == NULL) {
        return std::make_tuple((std::uintptr_t)nullptr,
                               bolt::ErrorSystemCall);
    }
    this->ptr = (std::uintptr_t)view;
    return std::make_tuple(ptr, bolt::Success);
}

bolt::ErrorCode FileImpl::Munmap(std::uintptr_t ptr) {
    if (this->ptr == ptr && ptr != NULL) {
        if (UnmapViewOfFile((LPCVOID)ptr)) {
            this->ptr = (std::uintptr_t)nullptr;
            return bolt::Success;
        }
        else {
            return bolt::ErrorSystemCall;
        }
    }
    else {
        return bolt::Success;
    }
}

} // namespace bolt::impl
#endif

#ifdef __linux__
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <error.h>

namespace bolt::impl {

int Getpagesize() { return (int)sysconf(_SC_PAGE_SIZE); }

struct FileImpl {
    FileImpl() : ptr(NULL), fd(-1), size(0){};
    ~FileImpl();
    std::tuple<std::uint64_t, bolt::ErrorCode> WriteAt(bolt::bytes buf,
                                                       std::uint64_t offset);
    std::tuple<std::uint64_t, bolt::ErrorCode>
    WriteAt(std::vector<bolt::bytes> &&bufs, std::uint64_t offset);
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
    int fd;

    void *ptr;                  // mmap ptr
    size_t size;                // mmap size
};

FileImpl::~FileImpl() {
    if (ptr != nullptr) {
        Munmap(std::uintptr_t(ptr));
    }
    if (fd != -1) {
        Close();
    }
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::WriteAt(bolt::bytes buf, std::uint64_t offset) {
    off_t ret = lseek(fd, offset, SEEK_SET);
    if (ret == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorSystemCall);
    }
    ssize_t sz = write(fd, buf.data(), buf.size());
    if (sz == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorSystemCall);
    } else if (sz < buf.size()) {
            return std::make_tuple(std::uint64_t(sz),
                                   bolt::ErrorSystemCall);
    }
    return std::make_tuple(std::uint64_t(sz), bolt::Success);
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::WriteAt(std::vector<bolt::bytes> &&bufs, std::uint64_t offset) {
    std::uint64_t total = 0;
    std::uint64_t written = 0;
    size_t length = 0;
    off_t ret;
    ssize_t sz;
    std::vector<struct iovec> vecs;
    vecs.reserve(IOV_MAX);
    while (length < bufs.size()) {
        vecs.clear();
        written = 0;
        while (vecs.size() < IOV_MAX && length < bufs.size()) {
            struct iovec item;
            item.iov_base = bufs[length].data();
            item.iov_len = bufs[length].size();
            written += item.iov_len;
            vecs.push_back(item);
            length++;
        }
        ret = lseek(fd, offset + total, SEEK_SET);
        if (ret == -1) {
            fmt::println("pwritev return {} {}, length: {}, IOV_MAX: {}", sz,
                         strerror(errno), vecs.size(), IOV_MAX);
            return std::make_tuple(std::uint64_t(0), bolt::ErrorSystemCall);
        }
        sz = pwritev(fd, vecs.data(), vecs.size(), offset + total);
        if (sz == -1) {
            return std::make_tuple(std::uint64_t(0), bolt::ErrorSystemCall);
        } else if (sz < written) {
            fmt::println("expected written {} bytes, write {}", total, sz);
            return std::make_tuple(total + written, bolt::ErrorSystemCall);
        }
        total += sz;
    }
    return std::make_tuple(total, bolt::Success);
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::ReadAt(bolt::bytes buf, std::uint64_t offset) {
    off_t ret = lseek(fd, offset, SEEK_SET);
    if (ret == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorSystemCall);
    }
    ssize_t sz = read(fd, buf.data(), buf.size());
    if (sz == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorSystemCall);
    } else if (sz < buf.size()) {
        return std::make_tuple(std::uint64_t(sz), bolt::ErrorDatabaseEOF);
    }
    return std::make_tuple(std::uint64_t(sz), bolt::Success);
}

bolt::ErrorCode FileImpl::Open(std::string path, bool readOnly) {
    int flag = O_CREAT;
    if (readOnly) {
        flag |= O_RDONLY;
    } else {
        flag |= O_RDWR;
    }
    fd = open(path.c_str(), flag, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd == -1) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Truncate(std::uint64_t size) {
    if (ftruncate(fd, (off_t)size)) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Close() {
    if (close(fd)) {
        return bolt::ErrorSystemCall;
    }
    fd = -1;
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Flock(bool exclusive, std::chrono::milliseconds timeout) {
    int flag = LOCK_SH;
    if (exclusive) {
        flag = LOCK_EX;
    }
    auto start = std::chrono::system_clock::now();
    while (true) {
        auto since = std::chrono::system_clock::now();
        if (timeout > 1us &&
            std::chrono::duration_cast<std::chrono::milliseconds>(
                since - start) > timeout) {
            return bolt::ErrorTimeout;
        }
        int ret = flock(fd, flag | LOCK_NB);
        if (!ret) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Funlock() {
    int ret = flock(fd, LOCK_UN);
    if (!ret) {
        return bolt::Success;
    }
    return bolt::ErrorSystemCall;
}

std::tuple<std::uintptr_t, bolt::ErrorCode> FileImpl::Mmap(std::uint64_t size) {
    ptr = mmap(NULL, size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    if (ptr == (void *)-1) {
        return std::make_tuple(std::uintptr_t(nullptr), bolt::ErrorSystemCall);
    }
    this->size = (size_t)size;
    return std::make_tuple(std::uintptr_t(ptr), bolt::Success);
}

bolt::ErrorCode FileImpl::Munmap(std::uintptr_t ptr) {
    if (this->ptr == (void*)ptr && this->ptr != NULL) {
        int ret = munmap((void *)ptr, (size_t)size);
        if (ret) {
            return bolt::ErrorSystemCall;
        }
    }
    this->ptr = NULL;
    this->size = 0;
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Fdatasync() {
    int ret = fdatasync(fd);
    if (ret) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

bolt::ErrorCode FileImpl::Fsync() {
    int ret = fsync(fd);
    if (ret) {
        return bolt::ErrorSystemCall;
    }
    return bolt::Success;
}

std::tuple<std::uint64_t, bolt::ErrorCode> FileImpl::Size() {
    struct stat buf;
    int ret = fstat(fd, &buf);
    if (ret) {
        return std::make_tuple(std::uint64_t(0), bolt::ErrorSystemCall);
    }
    return std::make_tuple(std::uint64_t(buf.st_size), bolt::Success);
}

} // namespace bolt::impl
#endif

namespace bolt::impl {

File::File() : pImpl(std::make_unique<impl::FileImpl>()) {
}

File::~File() {
};

std::tuple<std::uint64_t, bolt::ErrorCode> File::WriteAt(bolt::bytes buf,
                                                         std::uint64_t offset) {
    return pImpl->WriteAt(buf, offset);
}

std::tuple<std::uint64_t, bolt::ErrorCode>
File::WriteAt(std::vector<bolt::bytes> &&bufs, std::uint64_t offset) {
    return pImpl->WriteAt(std::move(bufs), offset);
}

std::tuple<std::uint64_t, bolt::ErrorCode> File::ReadAt(bolt::bytes buf,
                                                        std::uint64_t offset) {
    return pImpl->ReadAt(buf, offset);
}

bolt::ErrorCode File::Fdatasync() { return pImpl->Fdatasync(); }

bolt::ErrorCode File::Fsync() { return pImpl->Fsync(); }

bolt::ErrorCode File::Flock(bool exclusive, std::chrono::milliseconds timeout) {
    return pImpl->Flock(exclusive, timeout);
}

bolt::ErrorCode File::Funlock() {
    return pImpl->Funlock();
}

bolt::ErrorCode File::Open(std::string path, bool readOnly) {
    return pImpl->Open(path, readOnly);
}

std::tuple<std::uint64_t, bolt::ErrorCode> File::Size() {
    return pImpl->Size();
}

std::tuple<std::uintptr_t, bolt::ErrorCode> File::Mmap(std::uint64_t size) {
    return pImpl->Mmap(size);
}

bolt::ErrorCode File::Munmap(std::uintptr_t ptr) { return pImpl->Munmap(ptr); }

bolt::ErrorCode File::Close() { return pImpl->Close(); }

bolt::ErrorCode File::Truncate(std::uint64_t size) {
    return pImpl->Truncate(size);
}

} // namespace bolt::impl
