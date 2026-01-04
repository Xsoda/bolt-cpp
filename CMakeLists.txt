cmake_minimum_required(VERSION 3.5)
project(bolt-cpp CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

add_executable(${PROJECT_NAME})

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

target_sources(${PROJECT_NAME}
  PRIVATE
  src/page.cpp
  src/page.hpp
  src/meta.cpp
  src/meta.hpp
  src/freelist.cpp
  src/freelist.hpp
  src/bucket.cpp
  src/bucket.hpp
  src/tx.cpp
  src/tx.hpp
  src/db.cpp
  src/db.hpp
  src/node.cpp
  src/node.hpp
  src/batch.cpp
  src/batch.hpp
  src/file.cpp
  src/file.hpp
  src/cursor.cpp
  src/cursor.hpp
)

target_sources(${PROJECT_NAME}
  PRIVATE
  test/main.cpp
  test/page_test.cpp
  test/freelist_test.cpp
  test/node_test.cpp
  test/db_test.cpp
  test/test.hpp
)


include_directories("${CMAKE_CURRENT_SOURCE_DIR}/test" "${CMAKE_CURRENT_SOURCE_DIR}/src")
