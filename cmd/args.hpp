#pragma once

#include "fmt/format.h"
#include "fmt/ranges.h"
#include <variant>
#include <string>
#include <algorithm>
#include <cstring>
#include <map>
#include <optional>

using ArgumentValue = std::variant < std::string, long long, bool, double,
      std::nullptr_t,
      std::vector<std::string>, std::vector<long long>, std::vector<double>>;

using CommandLineArgument = std::map<std::string, ArgumentValue>;

inline bool compare_string(const std::string &a, const std::string &b) {
    if (a.size() != b.size()) {
        return false;
    }
    return strncasecmp(a.data(), b.data(), a.size()) == 0;
}

inline ArgumentValue Parse(std::string str) {
    ArgumentValue value;

    if (compare_string(str, "on") || compare_string(str, "true")) {
        return true;
    }

    if (compare_string(str, "off") || compare_string(str, "false")) {
        return false;
    }

    try {
        std::size_t pos = 0;
        auto integer = std::stoll(str, &pos);
        if (pos == str.size()) {
          return integer;
        }
    } catch (...) {
    }

    try {
        std::size_t pos = 0;
        auto dbl = std::stod(str, &pos);
        if (pos == str.size()) {
            return dbl;
        }
    } catch (...) {
    }

    return str;
}

// parse --name=value, --name value and --name
// e.g.: parse `--a 1 --b on --c --float 3.14 --integer 1 2 3`
//   => { "a": 1, "b": true, "c": null, "float": 3.14, "integer": [1, 2, 3] }
inline CommandLineArgument Parse(int argc, char **argv) {
    CommandLineArgument cmd;
    std::string prev;
    for (int i = 0; i < argc; i++) {
        std::string str = argv[i];
        if (str.find("--") == 0) {
            if (auto pos = str.find('='); pos != std::string::npos) {
                auto key = str.substr(2, pos - 2);
                auto value = Parse(str.substr(pos + 1));
                cmd[key] = value;
                prev = key;
                continue;
            }
            if (i + 1 < argc) {
                auto key = str.substr(2);
                std::string next = argv[i + 1];
                if (next.find("--") == 0) {
                    cmd[key] = nullptr;
                    prev = key;
                } else {
                    auto value = Parse(argv[i + 1]);
                    cmd[key] = value;
                    prev = key;
                    i++;
                }
            } else {
                auto key = str.substr(2);
                cmd[key] = nullptr;
                prev = key;
            }
        } else {
            // build array arguments, e.g. --value 0 1 2 3
            auto it = cmd.find(prev);
            if (it == cmd.end()) {
                continue;
            }

            if (auto p = std::get_if<std::string>(&it->second)) {
                std::vector<std::string> container({*p, argv[i]});
                cmd[prev] = container;
                continue;
            } else if (auto p = std::get_if<std::vector<std::string>>(
                           &it->second)) {
                p->push_back(argv[i]);
                continue;
            }

            auto value = Parse(argv[i]);
            if (auto v = std::get_if<double>(&value)) {
                if (auto p = std::get_if<double>(&it->second)) {
                    std::vector<double> container({*p, *v});
                    cmd[prev] = container;
                } else if (auto p =
                               std::get_if<std::vector<double>>(&it->second)) {
                    p->push_back(*v);
                }
            } else if (auto v = std::get_if<long long>(&value)) {
                if (auto p = std::get_if<long long>(&it->second)) {
                    std::vector<long long> container({*p, *v});
                    cmd[prev] = container;
                } else if (auto p = std::get_if<std::vector<long long>>(
                               &it->second)) {
                    p->push_back(*v);
                }
            }
        }
    }
    return cmd;
}

template <typename T>
std::optional<T> GetArgument(const CommandLineArgument &cmd,
                             const std::string &key) {
    auto it = cmd.find(key);
    if (it == cmd.end()) {
        return std::nullopt;
    }
    if (auto v = std::get_if<T>(&it->second)) {
        return *v;
    }
    return std::nullopt;
}

FMT_BEGIN_NAMESPACE
template <>
struct formatter<ArgumentValue> : nested_formatter<fmt::string_view> {
    auto format(ArgumentValue value, format_context &ctx) const
        -> decltype(ctx.out()) {
        return write_padded(ctx, [this, value](auto out) -> decltype(out) {
            if (auto v = std::get_if<bool>(&value)) {
                if (*v) {
                    return fmt::format_to(out, "<BOOL:true>");
                } else {
                    return fmt::format_to(out, "<BOOL:false>");
                }
            } else if (auto v = std::get_if<std::string>(&value)) {
                return fmt::format_to(out, "<STR:{}>", *v);
            } else if (auto v = std::get_if<double>(&value)) {
                return fmt::format_to(out, "<DOUBLE:{}>", *v);
            } else if (auto v = std::get_if<long long>(&value)) {
                return fmt::format_to(out, "<INTEGER:{}>", *v);
            } else if (auto v = std::get_if<std::nullptr_t>(&value)) {
                return fmt::format_to(out, "<NULL>");
            } else if (auto v = std::get_if<std::vector<std::string>>(&value)) {
                return fmt::format_to(out, "{}", *v);
            } else if (auto v = std::get_if<std::vector<double>>(&value)) {
                return fmt::format_to(out, "{}", *v);
            } else if (auto v = std::get_if<std::vector<long long>>(&value)) {
                return fmt::format_to(out, "{}", *v);
            }else {
                return fmt::format_to(out, "<UNKNOWN>");
            }
        });
    };
};
FMT_END_NAMESPACE
