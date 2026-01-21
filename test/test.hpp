/**
 * Copyright (c) 2025 Michal Sledz
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#include <functional>
#include <iostream>
#include <fmt/format.h>
#include <fmt/ostream.h>

class TestResult {
public:
    bool success;
    std::string err_reason;

    template <typename... T>
    TestResult(fmt::format_string<T...> fmt, T &&...args) {
        success = false;
        err_reason = fmt::format(fmt, args...);
    }

    TestResult(std::string err_reason) : success(false), err_reason(err_reason) {}
    TestResult(bool success, std::string err_reason = "") : success(success), err_reason(err_reason) {}
};

class Test {
public:
    std::string name;
    std::function<TestResult(void)> f;

    Test(std::string name, std::function<TestResult(void)> testFunc) : name(name), f(testFunc) {}

    TestResult run() {
        fmt::println("\n*** Running {} test", name);
        TestResult res = this->f();
        if (res.success) {
            fmt::println("*** Finished {} test", name);
        } else {
            fmt::println(std::cerr, "{} test failed. Reason: {}", name, res.err_reason);
        }

        return res;
    }
};
