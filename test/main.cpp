#include "test.hpp"
#include <chrono>

TestResult TestNodePut();

static const std::vector<Test> tests = {
    Test("Test Node Put", TestNodePut),
};

int main(int argc, char **argv) {
  int success_tests = 0;
  int failed_tests = 0;
  std::chrono::steady_clock::time_point startTime, endTime;
  startTime = std::chrono::steady_clock::now();
  for (auto test : tests) {
    auto res = test.run();
    if (res.success) {
        success_tests++;
    } else {
        failed_tests++;
    }
  }
  endTime = std::chrono::steady_clock::now();

  auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      endTime - startTime);
  auto durationS =
      std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
  std::cout << "Finished " << success_tests + failed_tests << " tests in "
            << durationS.count() << "s (" << durationMs.count()
            << "ms). Succeed: " << success_tests << ". Failed: " << failed_tests
            << "." << std::endl;
  return 0;
}
