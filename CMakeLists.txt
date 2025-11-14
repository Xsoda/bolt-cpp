cmake_minimum_required(VERSION 3.5)
project(bolt-cpp CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(PROJECT_SOURCE
  src/page.cpp
  src/meta.cpp
  src/main.cpp)

add_executable(${PROJECT_NAME} ${PROJECT_SOURCE})
