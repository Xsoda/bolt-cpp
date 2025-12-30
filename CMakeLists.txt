cmake_minimum_required(VERSION 3.5)
project(bolt-cpp CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(${PROJECT_NAME})

target_sources(${PROJECT_NAME}
  PRIVATE
  src/page.cpp
  src/meta.cpp
  src/freelist.cpp
)

target_sources(${PROJECT_NAME}
  PRIVATE
  test/main.cpp
  test/page_test.cpp
  test/freelist_test.cpp
  test/test.hpp
)


include_directories("${CMAKE_CURRENT_SOURCE_DIR}/test" "${CMAKE_CURRENT_SOURCE_DIR}/src")
