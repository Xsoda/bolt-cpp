#include "bolt/error.hpp"

namespace bolt {

class ErrorCategory : public std::error_category {
public:
    static const ErrorCategory &instance() {
        static ErrorCategory inst;
        return inst;
    }
    const char *name() const noexcept override { return "bolt"; }
    std::string message(int ec) const override {
        return fmt::format("{}", static_cast<bolt::ErrorCode>(ec));
    }
};

std::error_code make_error_code(bolt::ErrorCode ec) {
    return {static_cast<int>(ec), bolt::ErrorCategory::instance()};
}

} // namespace bolt
