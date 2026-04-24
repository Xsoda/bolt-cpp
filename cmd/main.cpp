#include "bolt/bolt.hpp"
#include <algorithm>
#include <filesystem>
#include <iterator>
#include <locale>
#include <span>

inline std::vector<std::string> string_split(const std::string &str, const std::string &delimiter) {
    std::vector<std::string> result;
    std::size_t start = 0;
    while (true) {
        std::size_t pos = str.find(delimiter, start);
        if (pos == std::string::npos) {
            result.push_back(str.substr(start));
            break;
        }
        result.push_back(str.substr(start, pos - start));
        start = pos + delimiter.size();
    }
    return result;
}

inline bool string_startswith(const std::string &str, const std::string &start) {
    return str.find(start) == 0;
}

inline bool string_endswith(const std::string &str, const std::string &end) {
    if (end.size() > str.size()) {
        return false;
    }
    std::size_t pos = str.size() - end.size();
    return str.find(end, pos) == pos;
}

inline std::tuple<std::optional<std::string>, std::optional<std::string>, bool>
ParseKeyRange(const std::string &str) {
    auto pos = str.find("...");
    if (pos == std::string::npos) {
        return {std::nullopt, std::nullopt, false};
    }
    auto start = str.substr(0, pos);
    auto stop = str.substr(pos + 3);
    if (start.empty()) {
        if (stop.empty()) {
            return {std::nullopt, std::nullopt, true};
        } else {
            return {std::nullopt, stop, true};
        }
    } else {
        if (stop.empty()) {
            return {start, std::nullopt, true};
        } else {
            return {start, stop, true};
        }
    }
}

int Help(int argc, char **argv) {
    fmt::println(R"(Usage:
  {} <command> <argument>...

Support command:
  list-bucket   <database>
  delete-bucket <database> <bucket-path>
  create-bucket <database> <bucket-path>
  set           <database> <bucket-path> <key> <value>
  get           <database> <bucket-path> <key-range>|<key>
  del           <database> <bucket-path> <key-range>|<key>
  help

<key-range> format:
  <start-key>...<end-key>

<bucket-path> format:
  <bucket-name>/<bucket-name>/<bucket-name>

version: {})",
                 argv[0], bolt::library_version());
    return 0;
}

int Set(int argc, char **argv) {
    bolt::DB db;
    if (argc < 4) {
        fmt::println("missing arguments");
        return -1;
    }
    std::string database = argv[0];
    std::string path = argv[1];
    std::string key = argv[2];
    std::string value = argv[3];
    if (auto err = db.Open(database); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    if (auto err = db.Update([&key, &value, &path](bolt::Tx tx) -> bolt::ErrorCode {
            auto [bucket, err] = tx.CreateBucketWithPath(path);
            if (err != bolt::ErrorCode::Success) {
                return err;
            }
            fmt::println("set <KEY-VALUE> `{}` => `{}`", key, value);
            return bucket.Put(key, value);
        });
        err != bolt::ErrorCode::Success) {
        fmt::println("update database fail, {}", err);
    }
    return 0;
}

int Del(int argc, char **argv) {
    bolt::DB db;
    if (argc < 3) {
        fmt::println("missing arguments");
        return -1;
    }
    std::string database = argv[0];
    std::string path = argv[1];
    std::string key = argv[2];

    if (auto err = db.Open(database); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    auto [start, stop, range] = ParseKeyRange(key);
    if (range) {
        if (auto err = db.Update([&path, &start, &stop](bolt::Tx tx) -> bolt::ErrorCode {
                auto [bucket, err] = tx.RetrieveBucketWithPath(path);
                if (err != bolt::ErrorCode::Success) {
                    return err;
                }
                bolt::const_bytes key, value;
                auto cursor = bucket.Cursor();
                if (start) {
                    std::tie(key, value) = cursor.Seek(bolt::to_bytes(*start));
                } else {
                    std::tie(key, value) = cursor.First();
                }
                while (!key.empty()) {
                    if (stop) {
                        auto ends = bolt::to_bytes(*stop);
                        auto cmp = std::lexicographical_compare_three_way(key.begin(), key.end(),
                                                                          ends.begin(), ends.end());
                        if (std::is_gteq(cmp)) {
                            break;
                        }
                    }
                    if (value.empty()) {
                        fmt::println("delete <BUCKET> `{}`", key);
                        if (auto err = bucket.DeleteBucket(key); err != bolt::ErrorCode::Success) {
                            return err;
                        }
                    } else {
                        fmt::println("delete <KEY-VALUE> `{}`", key);
                        cursor.Delete();
                    }
                    std::tie(key, value) = cursor.Next();
                }
                return bolt::ErrorCode::Success;
            });
            err != bolt::ErrorCode::Success) {
            fmt::println("update database fail, {}", err);
        }
    } else {
        if (auto err = db.Update([&key, &path](bolt::Tx tx) -> bolt::ErrorCode {
                auto [bucket, err] = tx.RetrieveBucketWithPath(path);
                if (err != bolt::ErrorCode::Success) {
                    return err;
                }
                fmt::println("delete <KEY-VALUE> `{}`", key);
                return bucket.Delete(key);
            });
            err != bolt::ErrorCode::Success) {
            fmt::println("update database fail, {}", err);
        }
    }
    return 0;
}

int Get(int argc, char **argv) {
    bolt::DB db;
    if (argc < 3) {
        fmt::println("missing arguments");
        return -1;
    }
    std::string database = argv[0];
    std::string path = argv[1];
    std::string key = argv[2];
    if (auto err = db.Open(database, true); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    auto [start, stop, range] = ParseKeyRange(key);
    if (range) {
        if (auto err = db.View([&path, &start, &stop](bolt::Tx tx) -> bolt::ErrorCode {
                auto [bucket, err] = tx.RetrieveBucketWithPath(path);
                if (err != bolt::ErrorCode::Success) {
                    return err;
                }
                bolt::const_bytes key, value;
                auto cursor = bucket.Cursor();
                if (start) {
                    std::tie(key, value) = cursor.Seek(bolt::to_bytes(*start));
                } else {
                    std::tie(key, value) = cursor.First();
                }
                while (!key.empty()) {
                    if (stop) {
                        auto ends = bolt::to_bytes(*stop);
                        auto cmp = std::lexicographical_compare_three_way(key.begin(), key.end(),
                                                                          ends.begin(), ends.end());
                        if (std::is_gteq(cmp)) {
                            break;
                        }
                    }
                    if (value.empty()) {
                        fmt::println("<BUCKET> `{}`", key);
                    } else {
                        fmt::println("<KEY-VALUE> `{}` => `{}`", key, value);
                    }
                    std::tie(key, value) = cursor.Next();
                }
                return bolt::ErrorCode::Success;
            });
            err != bolt::ErrorCode::Success) {
            fmt::println("retrieve key fail, {}", err);
        }
    } else {
        if (auto err = db.View([&key, &path](bolt::Tx tx) -> bolt::ErrorCode {
                auto [bucket, err] = tx.RetrieveBucketWithPath(path);
                if (err != bolt::ErrorCode::Success) {
                    return err;
                }
                auto value = bucket.Get(key);
                if (value.empty()) {
                    fmt::println("<KEY-VALUE> `{}` not found", key);
                } else {
                    fmt::println("<KEY-VALUE> `{}` => `{}`", key, value);
                }
                return bolt::ErrorCode::Success;
            });
            err != bolt::ErrorCode::Success) {
            fmt::println("retrieve key fail, {}", err);
        }
    }
    return 0;
}

struct BucketTree {
    std::string name;
    std::vector<BucketTree> children;
};

int ListBucket(int argc, char **argv) {
    bolt::DB db;
    if (argc < 1) {
        fmt::println("missing database argument");
        return -1;
    }
    std::string database = argv[0];
    if (auto err = db.Open(database, true); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    std::function<std::vector<BucketTree>(bolt::Bucket &)> list_bucket;
    list_bucket = [&list_bucket](bolt::Bucket &b) -> std::vector<BucketTree> {
        std::vector<BucketTree> result;
        auto cursor = b.Cursor();
        auto [key, value] = cursor.First();
        while (!key.empty()) {
            if (value.empty() && value.data() == nullptr) {
                BucketTree item;
                item.name = std::string(reinterpret_cast<const char *>(key.data()), key.size());
                auto subbucket = b.RetrieveBucket(key);
                item.children = list_bucket(subbucket);
                result.push_back(item);
            }
            std::tie(key, value) = cursor.Next();
        };
        return result;
    };
    BucketTree root;
    root.name = db.Path();
    if (auto err = db.View([&list_bucket, &root](bolt::Tx tx) -> bolt::ErrorCode {
            tx.ForEach([&](bolt::const_bytes name, bolt::Bucket b) -> bolt::ErrorCode {
                BucketTree item;
                item.name = std::string(reinterpret_cast<const char *>(name.data()), name.size());
                item.children = list_bucket(b);
                root.children.push_back(item);
                return bolt::ErrorCode::Success;
            });
            return bolt::ErrorCode::Success;
        });
        err != bolt::ErrorCode::Success) {
        fmt::println("view database fail, {}", err);
    }
    // ┌─┬─┐
    // ├─┼─┤
    // │ │ │
    // └─┴─┘
    auto string_repeat = [](const std::string &s, int n) -> std::string {
        std::string result;
        for (int i = 0; i < n; i++) {
            result += s;
        }
        return result;
    };
    std::function<void(BucketTree &, std::string)> print_bucket;
    print_bucket = [&print_bucket, &string_repeat](BucketTree &tree, std::string prefix) {
        for (int i = 0; i < tree.children.size(); i++) {
            auto &item = tree.children[i];
            if (i == tree.children.size() - 1) {
                fmt::println("{}└── {}", prefix, item.name);
                print_bucket(item, prefix + "    ");
            } else {
                fmt::println("{}├── {}", prefix, item.name);
                print_bucket(item, prefix + "│   ");
            }
        }
    };
    fmt::println("┌{}┐", string_repeat("─", root.name.size() + 2));
    fmt::println("│ {} │", root.name);
    fmt::println("├{}┘", string_repeat("─", root.name.size() + 2));
    print_bucket(root, "");
    return 0;
}

int DeleteBucket(int argc, char **argv) {
    bolt::DB db;
    if (argc < 2) {
        fmt::println("missing argument");
        return -1;
    }
    std::string database = argv[0];
    std::string path = argv[1];
    if (auto err = db.Open(database); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    if (path.empty()) {
        fmt::println("missing bucket path");
        return -1;
    }
    if (auto err = db.Update([&path](bolt::Tx tx) -> bolt::ErrorCode {
            auto bucket_names = string_split(path, "/");
            if (bucket_names.size() == 1) {
                fmt::println("delete bucket `{}`", bucket_names.front());
                return tx.DeleteBucket(bucket_names[0]);
            }
            bolt::Bucket bucket = tx.Bucket(bucket_names[0]);
            for (int i = 1; i < bucket_names.size() - 1; i++) {
                bucket = bucket.RetrieveBucket(bucket_names[i]);
            }
            fmt::println("delete bucket `{}`", bucket_names.back());
            return bucket.DeleteBucket(bucket_names.back());
        });
        err != bolt::ErrorCode::Success) {
        fmt::println("update database fail, {}", err);
    }
    return 0;
}

int CreateBucket(int argc, char **argv) {
    bolt::DB db;
    if (argc < 2) {
        fmt::println("missing argument");
        return -1;
    }
    std::string database = argv[0];
    std::string path = argv[1];
    if (auto err = db.Open(database); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    if (auto err = db.Update([&path](bolt::Tx tx) -> bolt::ErrorCode {
            if (auto [bucket, err] = tx.CreateBucketWithPath(path);
                err != bolt::ErrorCode::Success) {
                return err;
            }
            return bolt::ErrorCode::Success;
        });
        err != bolt::ErrorCode::Success) {
        fmt::println("update database fail, {}", err);
    }
    return 0;
}

int StoreFolder(int argc, char **argv) {
    bolt::DB db;
    std::string database = argv[0];
    std::string folder = argv[1];
    if (auto err = db.Open(database); err != bolt::ErrorCode::Success) {
        fmt::println("open database fail, {}", err);
        return -1;
    }
    std::filesystem::path p = folder;
    p = p.lexically_normal();
    if (!std::filesystem::exists(p)) {
        fmt::println("{} not exists", folder);
        return -1;
    }
    auto status = std::filesystem::status(p);
    if (!std::filesystem::is_directory(status)) {
        fmt::println("{} is not directory", folder);
        return -1;
    }
    std::function<bolt::ErrorCode(bolt::Bucket & b, std::filesystem::path p)> iter_folder;
    iter_folder = [&iter_folder](bolt::Bucket &b, std::filesystem::path p) -> bolt::ErrorCode {
        for (const auto &it : std::filesystem::directory_iterator{p}) {
            auto filename = it.path().filename().string();
            if (it.is_directory()) {
                auto [sub, err] = b.CreateBucketIfNotExists(filename);
                if (err != bolt::ErrorCode::Success) {
                    return err;
                }
                if (err = iter_folder(sub, it.path()); err != bolt::ErrorCode::Success) {
                    return err;
                }
            } else if (it.is_regular_file()) {
                std::ifstream fin{it.path(), std::ios::binary | std::ios::ate};
                if (!fin.is_open()) {
                    return bolt::ErrorCode::ErrorSystemCall;
                }
                std::streamsize filesize = fin.tellg();
                fin.seekg(0, std::ios::beg);
                std::vector<char> buf(filesize);
                if (!fin.read(buf.data(), filesize)) {
                    return bolt::ErrorCode::ErrorSystemCall;
                }
                if (auto err = b.Put(bolt::to_bytes(filename), bolt::to_bytes(buf));
                    err != bolt::ErrorCode::Success) {
                    return err;
                }
            }
        }
        return bolt::ErrorCode::Success;
    };
    if (auto err = db.Update([&iter_folder, &p](bolt::Tx tx) -> bolt::ErrorCode {
            auto filename = p.filename().string();
            if (filename.empty()) {
                filename = p.parent_path().filename().string();
            }
            auto [b, err] = tx.CreateBucketIfNotExists(filename);
            if (err != bolt::ErrorCode::Success) {
                return err;
            }
            return iter_folder(b, p);
        });
        err != bolt::ErrorCode::Success) {
        fmt::println("update database fail, {}", err);
        return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        std::string command = argv[1];
        if (command == "list-bucket") {
            return ListBucket(argc - 2, argv + 2);
        } else if (command == "delete-bucket") {
            return DeleteBucket(argc - 2, argv + 2);
        } else if (command == "create-bucket") {
            return CreateBucket(argc - 2, argv + 2);
        } else if (command == "help") {
            Help(argc, argv);
        } else if (command == "get") {
            return Get(argc - 2, argv + 2);
        } else if (command == "set") {
            return Set(argc - 2, argv + 2);
        } else if (command == "del") {
            return Del(argc - 2, argv + 2);
        } else if (command == "store-folder") {
            return StoreFolder(argc - 2, argv + 2);
        } else {
            Help(argc, argv);
        }
    } else {
        Help(argc, argv);
    }
    return 0;
}
