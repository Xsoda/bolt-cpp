#include "freelist.hpp"
#include "page.hpp"
#include "utils.hpp"
#include "error.hpp"
#include "test.hpp"
#include <cstring>

TestResult TestFreelist_free() {
    bolt::impl::page page{12};
    bolt::impl::freelist freelist;
    freelist.free(100, &page);
    auto it = freelist.pending.find(100);
    if (it == freelist.pending.end()) {
      return TestResult(false, "freelist free fail");
    }
    if (freelist.pending_count() != 1) {
        return TestResult(false, "freelist pending count");
    }
    auto vec = std::vector<bolt::impl::pgid>({12});
    for (int i = 0; i < vec.size(); i++) {
        if (it->second[i] != vec[i]) {
            return TestResult(false, "freelist pending page");
        }
    }
    return true;
}

TestResult TestFreelist_free_overflow() {
    bolt::impl::page page{12, 3};
    bolt::impl::freelist freelist;
    freelist.free(100, &page);
    auto it = freelist.pending.find(100);
    if (it == freelist.pending.end()) {
        return TestResult(false, "freelist free fail");
    }
    if (freelist.pending_count() != 4) {
        return TestResult(false, "freelist pending count");
    }
    auto vec = std::vector<bolt::impl::pgid>({12, 13, 14, 15});
    for (int i = 0; i < vec.size(); i++) {
        if (it->second[i] != vec[i]) {
            return TestResult(false, "freelist pending page");
        }
    }
    return true;
}

TestResult TestFreelist_release() {
    bolt::impl::page p1{12, 1};
    bolt::impl::page p2{9};
    bolt::impl::page p3{39};
    bolt::impl::freelist freelist;
    freelist.free(100, &p1);
    freelist.free(100, &p2);
    freelist.free(102, &p3);
    freelist.release(100);
    freelist.release(101);
    auto vec = std::vector<bolt::impl::pgid>({9, 12, 13});
    if (freelist.ids.size() != vec.size()) {
        return TestResult(false, "freelist ids count not equal");
    }
    for (int i = 0; i < vec.size(); i++) {
        if (freelist.ids[i] != vec[i]) {
            return TestResult(false, "freelist ids item mismatch");
        }
    }

    freelist.release(102);
    vec = std::vector<bolt::impl::pgid>({9, 12, 13, 39});
    if (freelist.ids.size() != vec.size()) {
        return TestResult(false, "freelist ids count not equal");
    }
    for (int i = 0; i < vec.size(); i++) {
        if (freelist.ids[i] != vec[i]) {
            return TestResult(false, "freelist ids item mismatch");
        }
    }
    return true;
}

TestResult TestFreelist_allocate() {
    bolt::impl::freelist freelist;
    freelist.ids = std::vector<bolt::impl::pgid>({3, 4, 5, 6, 7, 9, 12, 13, 18});
    auto id = freelist.allocate(3);
    if (id != 3) {
        return TestResult(false, "freelist allocate(3) expect result 3");
    }
    id = freelist.allocate(1);
    if (id != 6) {
        return TestResult(false, "freelist allocate(1) expect result 6");
    }
    id = freelist.allocate(3);
    if (id != 0) {
        return TestResult(false, "freelist allocate(3) expect result 0");
    }
    id = freelist.allocate(2);
    if (id != 12) {
        return TestResult(false, "freelist allocate(2) expect result 12");
    }
    id = freelist.allocate(1);
    if (id != 7) {
        return TestResult(false, "freelist allocate(1) expect result 7");
    }
    id = freelist.allocate(0);
    if (id != 0) {
        return TestResult(false, "freelist allocate(0) expect result 0");
    }
    std::vector<bolt::impl::pgid> vec = {9, 18};
    if (freelist.ids.size() != vec.size()) {
        return TestResult(false, "freelist ids count not equal");
    }
    for (int i = 0; i < vec.size(); i++) {
        if (freelist.ids[i] != vec[i]) {
            return TestResult(false, "freelist ids item mismatch");
        }
    }
    return true;
}

TestResult TestFreelist_read() {
    std::byte buf[4096];
    std::memset(buf, 0, sizeof(buf));
    bolt::impl::page *page = reinterpret_cast<bolt::impl::page *>(buf);
    page->flags = bolt::impl::freeListPageFlag;
    page->count = 2;
    std::span<bolt::impl::pgid> ids(&page->ptr, page->count);
    ids[0] = 23;
    ids[1] = 50;

    bolt::impl::freelist freelist;
    freelist.read(page);
    std::vector<bolt::impl::pgid> vec = {23, 50};
    if (freelist.ids.size() != vec.size()) {
        return TestResult(false, "freelist ids count not equal");
    }
    for (int i = 0; i < vec.size(); i++) {
        if (freelist.ids[i] != vec[i]) {
            return TestResult(false, "freelist ids item mismatch");
        }
    }
    return true;
}

TestResult TestFreelist_write() {
    std::byte buf[4096];
    std::memset(buf, 0, sizeof(buf));
    bolt::impl::freelist freelist;
    freelist.ids = std::vector<bolt::impl::pgid>({12, 39});
    freelist.pending[100] = std::vector<bolt::impl::pgid>({28, 11});
    freelist.pending[101] = std::vector<bolt::impl::pgid>({3});
    bolt::impl::page *page = reinterpret_cast<bolt::impl::page *>(buf);
    auto ret = freelist.write(page);
    if (ret != bolt::ErrorCode::Success) {
        return TestResult(false, "freelist write page fail");
    }

    bolt::impl::freelist freelist2;
    freelist.read(page);
    std::vector<bolt::impl::pgid> vec = {3, 11, 12, 28, 39};
    if (freelist.ids.size() != vec.size()) {
        return TestResult(false, "freelist ids count not equal");
    }
    for (int i = 0; i < vec.size(); i++) {
        if (freelist.ids[i] != vec[i]) {
            return TestResult(false, "freelist ids item mismatch");
        }
    }
    return true;
}
