#include "test.hpp"
#include "impl/page.hpp"
#include "impl/freelist.hpp"
#include "impl/utils.hpp"
#include "util.hpp"
#include <span>

TestResult TestPageType() {
    bolt::impl::page page;
    page.flags = bolt::impl::branchPageFlag;
    if (page.type() != "branch") {
        return TestResult(false, "expect page type is branch");
    }
    page.flags = bolt::impl::leafPageFlag;
    if (page.type() != "leaf") {
        return TestResult(false, "expect page type is leaf");
    }
    page.flags = bolt::impl::metaPageFlag;
    if (page.type() != "meta") {
        return TestResult(false, "expect page type is meta");
    }
    page.flags = bolt::impl::freeListPageFlag;
    if (page.type() != "freelist") {
        return TestResult(false, "expect page type is freelist");
    }
    page.flags = 20000;
    if (page.type() != "unknown") {
        return TestResult(false, "expect page type is unknown");
    }
    return true;
}

TestResult TestMergePgid() {
    auto a = std::vector<bolt::impl::pgid>({4, 5, 6, 10, 11, 12, 13, 27});
    auto b = std::vector<bolt::impl::pgid>({1, 3, 8, 9, 25, 30});
    std::vector<bolt::impl::pgid> c;
    c.assign(a.size() + b.size(), -1);
    bolt::impl::mergepgids(c, a, b);
    auto ret = std::vector<bolt::impl::pgid>(
        {1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30});
    if (ret.size() != c.size()) {
        return TestResult(false, "merge result length not equal");
    }
    if (!Equal(ret, c)) {
        return TestResult(false, "merge value mismatch");
    }
    for (int i = 0; i < ret.size(); i++) {
        if (ret[i] != c[i]) {
            return TestResult(false, "merge value mismatch");
        }
    }

    a = std::vector<bolt::impl::pgid>({4, 5, 6, 10, 11, 12, 13, 27, 35, 36});
    b = std::vector<bolt::impl::pgid>({8, 9, 25, 30});
    c.assign(a.size() + b.size(), -1);
    bolt::impl::mergepgids(c, a, b);
    ret = std::vector<bolt::impl::pgid>(
        {4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30, 35, 36});
    if (ret.size() != c.size()) {
        return TestResult(false, "merge result length not equal");
    }
    for (int i = 0; i < ret.size(); i++) {
        if (ret[i] != c[i]) {
            return TestResult(false, "merge value mismatch");
        }
    }
    return true;
}
