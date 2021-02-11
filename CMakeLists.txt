cmake_minimum_required(VERSION 3.5)

project(cydb)

set(CMAKE_CXX_STANDARD 17)

find_library(ROCKSDB_LIB rocksdb)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(cydb main.cpp)

target_link_libraries(cydb ${ROCKSDB_LIB})



