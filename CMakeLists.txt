cmake_minimum_required(VERSION 3.5)
project(bolt-cpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build bolt tests" OFF)
option(BUILD_CXX_CHAOS_TEST "Build cpp chaos test" OFF)
option(BUILD_GOLANG_CHAOS_TEST "Build golang chaos test" OFF)
option(BUILD_BOLT_CLI "Build bolt cli program" OFF)

find_program(CCACHE_PROGRAM NAMES "ccache")
if(CCACHE_PROGRAM)
  message(STATUS "Found ccache: ${CCACHE_PROGRAM}")
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

if(BUILD_GOLANG_CHAOS_TEST)
  find_program(GOLANG_PROGRAM NAMES "go")
  if(GOLANG_PROGRAM)
    message(STATUS "Found Golang: ${GOLANG_PROGRAM}")
    set(GO_BINARY_NAME "go-chaos-op")
    add_custom_target(${GO_BINARY_NAME} ALL
      COMMAND ${GOLANG_PROGRAM} build -o ${CMAKE_BINARY_DIR}/${GO_BINARY_NAME}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/golang
      SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/golang/main.go
      BYPRODUCTS ${CMAKE_BINARY_DIR}/${GO_BINARY_NAME}
      COMMENT "Building Go Binary: ${GO_BINARY_NAME}")
  endif()
endif()

add_library(bolt STATIC)

add_subdirectory(deps/fmt)
target_include_directories(bolt
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(bolt PRIVATE ${PEDANTIC_COMPILE_FLAGS})
target_link_libraries(bolt PRIVATE fmt::fmt)

if(MSVC)
  target_compile_definitions(bolt PRIVATE
    /W4
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

if(BUILD_TESTS)
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
endif()

if(BUILD_BOLT_CLI)
  add_executable(bolt-cli)
  target_sources(bolt-cli
    PRIVATE
    src/impl/page.hpp
    src/impl/page.cpp
    src/impl/file.hpp
    src/impl/file.cpp
    src/impl/utils.hpp
    src/impl/meta.hpp
    src/impl/meta.cpp
    cmd/args.hpp
    cmd/main.cpp)
  target_link_libraries(bolt-cli PRIVATE fmt::fmt bolt)
  target_include_directories(bolt-cli
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  if(MSVC)
  target_compile_definitions(bolt-cli PRIVATE
    /W4
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
endif()

if(BUILD_CXX_CHAOS_TEST)
  add_executable(chaos-op)
  target_sources(chaos-op
    PRIVATE
    cmd/args.hpp
    cmd/chaos-op.cpp)
  target_link_libraries(chaos-op PRIVATE fmt::fmt bolt)
endif()
