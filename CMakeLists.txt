cmake_minimum_required(VERSION 3.5)
project(bolt-cpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_program(CCACHE_PROGRAM ccache)

if(CCACHE_PROGRAM)
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

add_library(bolt)

add_subdirectory(deps/fmt)
target_include_directories(bolt
  PUBLIC
  SYSTEM .
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(bolt PRIVATE ${PEDANTIC_COMPILE_FLAGS})
target_link_libraries(bolt fmt::fmt)

if(MSVC)
  add_compile_definitions(
    /W3
    /utf-8
    NOMINMAX
    UNICODE
    _UNICODE
    _CRT_SECURE_NO_WARNINGS
    _CRT_NONSTDC_NO_WARNINGS
    $<$<CONFIG:DEBUG>:DEBUG>
    $<$<CONFIG:DEBUG>:_DEBUG>
)
endif()

target_sources(bolt
  PUBLIC
  FILE_SET HEADERS
  BASE_DIRS "include"
  FILES
  include/bolt/pimpl.hpp
  include/bolt/common.hpp
  include/bolt/error.hpp
  include/bolt/bolt.hpp
)

target_sources(bolt
  PRIVATE
  src/impl/utils.hpp
  src/impl/page.cpp
  src/impl/page.hpp
  src/impl/meta.cpp
  src/impl/meta.hpp
  src/impl/freelist.cpp
  src/impl/freelist.hpp
  src/impl/bucket.cpp
  src/impl/bucket.hpp
  src/impl/tx.cpp
  src/impl/tx.hpp
  src/impl/db.cpp
  src/impl/db.hpp
  src/impl/node.cpp
  src/impl/node.hpp
  src/impl/batch.cpp
  src/impl/batch.hpp
  src/impl/file.cpp
  src/impl/file.hpp
  src/impl/cursor.cpp
  src/impl/cursor.hpp
  src/impl/bsearch.hpp
  src/bolt.cpp
)

add_executable(bolt-tests)
target_link_libraries(bolt-tests PRIVATE bolt fmt::fmt)

target_sources(bolt-tests
  PRIVATE
  test/main.cpp
  test/page_test.cpp
  test/freelist_test.cpp
  test/node_test.cpp
  test/db_test.cpp
  test/cursor_test.cpp
  test/bucket_test.cpp
  test/tx_test.cpp
  test/test.hpp
  test/quick_check.hpp
  test/util.hpp
  test/random.hpp
  test/random.cpp
)

target_include_directories(bolt-tests
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/test
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
