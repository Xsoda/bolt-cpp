#include "file.hpp"
#include <chrono>
#include <thread>

#ifdef WIN32
#include <windows.h>

namespace bolt::platfrom {

int Getpagesize() {
  SYSTEM_INFO info;
  ::GetSystemInfo(&info);
  return (int)info.dwPageSize;
}

struct FileImpl {
    FileImpl() = default;
    std::tuple<std::uint64_t, bolt::ErrorCode> WriteAt(bolt::bytes buf,
                                                      std::uint64_t offset);
    std::tuple<std::uint64_t, bolt::ErrorCode> ReadAt(bolt::bytes buf,
                                                     std::uint64_t offset);
    bolt::ErrorCode Fdatasync();
    bolt::ErrorCode Flock(bool exclusive, std::chrono::milliseconds timeout);
    bolt::ErrorCode Funlock();
    bolt::ErrorCode Open(std::string path, bool readOnly);
    bolt::ErrorCode Close();
    bool::ErrorCode Truncate(std::uint64_t size);
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
    char *buf = new char[len];
    WideCharToMultiByte(CP_ACP, 0, ws.c_str(), slen, buf, len, NULL, NULL);
    std::string r(buf);
    delete[] buf;
    return r;
}

std::wstring str2wstr(const std::string &s) {
    int len;
    int slength = (int)s.length() + 1;
    len = MultiByteToWideChar(CP_ACP, 0, s.c_str(), slength, NULL, 0);
    wchar_t *buf = new wchar_t[len];
    MultiByteToWideChar(CP_ACP, 0, s.c_str(), slength, buf, len);
    std::wstring r(buf);
    delete[] buf;
    return r;
}

bolt::Error FileImpl::Truncate(std::uint64_t size) {
    LARGE_INTEGER li;
    li.QuadPart = size;
    if (!SetFilePointerEx(file, &li, NULL, FILE_BEGIN)) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    if (!SetEndOfFile(file)) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    return bolt::ErrorCode::Success;
}

std::tuple<std::uint64_t, bolt::ErrorCode> FileImpl::Size() {
    LARGE_INTEGER li;
    if (::GetFileSizeEx(file, &li)) {
        return std::make_tuple(std::uint64_t(li.QuadPart), bolt::ErrorCode::Success);
    }
    return std::make_tuple(std::uint64_t(0), bolt::ErrorCode::ErrorSystemCall);
}


bolt::ErrorCode FileImpl::Open(std::string path, bool readOnly) {
    this->path = str2wstr(path);
    DWORD dwDesiredAccess = GENERIC_READ;
    if (!readOnly) {
        dwDesiredAccess = GENERIC_WRITE;
    }
    file = CreateFile(this->path.c_str(), dwDesiredAccess, 0, NULL, OPEN_ALWAYS,
                      FILE_ATTRIBUTE_NORMAL, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
}

bolt::ErrorCode FileImpl::Close() {
    BOOL ret = CloseHandle(file);
    if (ret) {
        return bolt::ErrorCode::Success;
    }
    return bolt::ErrorCode::ErrorSystemCall;
}

bolt::ErrorCode FileImpl::Flock(bool exclusive,
                                std::chrono::milliseconds timeout) {
    DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
    if (exclusive) {
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    std::wstring lock_path = path;
    lock_path += L".lock";

    HANDLE l = CreateFile(lock_path.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                          NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
    if (lockfile == INVALID_HANDLE_VALUE) {
        return bolt::ErrorCode::ErrorSystemCall;
    }

    lockfile = l;
    auto start =
        std::chrono::system_clock::now();
    while (true) {
      auto since = std::chrono::system_clock::now();
      if (timeout > 1us &&
          std::chrono::duration_cast<std::chrono::milliseconds>(since - start) >
              timeout) {
          return bolt::ErrorCode::ErrorTimeout;
      }
      BOOL ret = LockFileEx(lockfile, flag, 0, 1, 0, NULL);
      if (ret) {
          break;
      } else {
          DWORD err = GetLastError();
          if (err != ERROR_LOCK_VIOLATION) {
              return bolt::ErrorCode::ErrorSystemCall;
          }
      }
      std::this_thread::sleep_for(50ms);
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode FileImpl::Funlock() {
    std::wstring lock_path = path;
    lock_path += L".lock";

    BOOL ret = UnlockFileEx(lockfile, 0, 0, 1, NULL);
    if (!ret) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    CloseHandle(lockfile);
    lockfile = INVALID_HANDLE_VALUE;
    ret = DeleteFile(lock_path.c_str());
    if (!ret) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode FileImpl::Fdatasync() {
    int ret = FlushFileBuffers(file) ? 0 : -1;
    if (ret == -1) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    return bolt::ErrorCode::Success;
}

std::tuple<std::uintptr_t, bolt::ErrorCode> FileImpl::Mmap(std::uint64_t size) {
    DWORD sizelo = (DWORD)(size >> 32);
    DWORD sizehi = (DWORD)(size & 0xFFFFFFFF);
    HANDLE fm =
        CreateFileMapping(file, NULL, PAGE_READONLY, sizelo, sizehi, NULL);
    if (fm == INVALID_HANDLE_VALUE) {
        return std::make_tuple(std::uintptr_t(nullptr),
                               bolt::ErrorCode::ErrorSystemCall);
    }
    LPVOID view = MapViewOfFile(fm, FILE_MAP_READ, 0, 0, size);
    CloseHandle(fm);
    if (view == NULL) {
        return std::make_tuple(std::uintptr_t(nullptr),
                               bolt::ErrorCode::ErrorSystemCall);
    }
    this->ptr = (std::uintptr_t)view;
    return std::make_tuple(std::uintptr_t(view), bolt::ErrorCode::Success);
}

bolt::ErrorCode FileImpl::Munmap(std::uintptr_t ptr) {
    if (UnmapViewOfFile((LPCVOID)ptr)) {
        this->ptr = nullptr;
        return bolt::ErrorCode::Success;
    } else {
        return bolt::ErrorCode::ErrorSystemCall;
    }
}

} // namespace bolt
#endif

#ifdef __linux__
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace bolt::platform {

int Getpagesize() { return (int)sysconf(_SC_PAGE_SIZE); }

struct FileImpl {
    FileImpl() : ptr(NULL), fd(-1), size(0){};
    ~FileImpl();
    std::tuple<std::uint64_t, bolt::ErrorCode> WriteAt(bolt::bytes buf,
                                                      std::uint64_t offset);
    std::tuple<std::uint64_t, bolt::ErrorCode> ReadAt(bolt::bytes buf,
                                                     std::uint64_t offset);
    bolt::ErrorCode Fdatasync();
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
    if (ptr != NULL) {
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
                               bolt::ErrorCode::ErrorSystemCall);
    }
    ssize_t sz = write(fd, buf.data(), buf.size());
    if (sz == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorCode::ErrorSystemCall);
    }
    return std::make_tuple(std::uint64_t(sz), bolt::ErrorCode::Success);
}

std::tuple<std::uint64_t, bolt::ErrorCode>
FileImpl::ReadAt(bolt::bytes buf, std::uint64_t offset) {
    off_t ret = lseek(fd, offset, SEEK_SET);
    if (ret == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorCode::ErrorSystemCall);
    }
    ssize_t sz = read(fd, buf.data(), buf.size());
    if (sz == -1) {
        return std::make_tuple(std::uint64_t(0),
                               bolt::ErrorCode::ErrorSystemCall);
    }
    return std::make_tuple(std::uint64_t(sz), bolt::ErrorCode::Success);
}

bolt::ErrorCode FileImpl::Open(std::string path, bool readOnly) {
    int flag = O_CREAT;
    if (readOnly) {
        flag |= O_RDONLY;
    }
    fd = open(path.c_str(), flag, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd == -1) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode FileImpl::Truncate(std::uint64_t size) {
    if (ftruncate(fd, (off_t)size)) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode FileImpl::Close() {
    if (close(fd)) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    fd = -1;
    return bolt::ErrorCode::Success;
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
            return bolt::ErrorCode::ErrorTimeout;
        }
        int ret = flock(fd, flag | LOCK_NB);
        if (!ret) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode FileImpl::Funlock() {
    int ret = flock(fd, LOCK_UN);
    if (!ret) {
        return bolt::ErrorCode::Success;
    }
    return bolt::ErrorCode::ErrorSystemCall;
}

std::tuple<std::uintptr_t, bolt::ErrorCode> FileImpl::Mmap(std::uint64_t size) {
    ptr = mmap(NULL, size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    if (ptr == (void *)-1) {
        return std::make_tuple(std::uintptr_t(nullptr), bolt::ErrorCode::ErrorSystemCall);
    }
    this->size = (size_t)size;
    return std::make_tuple(std::uintptr_t(ptr), bolt::ErrorCode::Success);
}

bolt::ErrorCode FileImpl::Munmap(std::uintptr_t ptr) {
    int ret = munmap((void *)ptr, (size_t)size);
    if (ret) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    this->ptr = NULL;
    this->size = 0;
    return bolt::ErrorCode::Success;
}

bolt::ErrorCode FileImpl::Fdatasync() {
    int ret = fdatasync(fd);
    if (ret) {
        return bolt::ErrorCode::ErrorSystemCall;
    }
    return bolt::ErrorCode::Success;
}

std::tuple<std::uint64_t, bolt::ErrorCode> FileImpl::Size() {
    struct stat buf;
    int ret = fstat(fd, &buf);
    if (ret) {
        return std::make_tuple(std::uint64_t(0), bolt::ErrorCode::ErrorSystemCall);
    }
    return std::make_tuple(std::uint64_t(buf.st_size), bolt::ErrorCode::Success);
}

}
#endif

namespace bolt {

File::File() : pImpl(std::make_unique<platform::FileImpl>()) {

}

std::tuple<std::uint64_t, bolt::ErrorCode> File::WriteAt(bolt::bytes buf,
                                                        std::uint64_t offset) {
    return pImpl->WriteAt(buf, offset);
}

std::tuple<std::uint64_t, bolt::ErrorCode> File::ReadAt(bolt::bytes buf,
                                                 std::uint64_t offset) {
    return pImpl->ReadAt(buf, offset);
}

bolt::ErrorCode File::Fdatasync() { return pImpl->Fdatasync(); }

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


}
