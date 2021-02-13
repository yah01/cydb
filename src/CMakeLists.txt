# the lib
add_library(cydb_lib SHARED "")
SET_TARGET_PROPERTIES(cydb_lib PROPERTIES LINKER_LANGUAGE CXX)

find_library(ROCKSDB_LIB rocksdb)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
target_link_libraries(cydb_lib ${ROCKSDB_LIB})

target_sources(
    cydb_lib
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/lib
    )
target_include_directories(
    cydb_lib
    PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}/lib
)

include_directories(lib)

# the server execution
add_executable(cydb_server bin/main.cpp)
target_link_libraries(cydb_server cydb_lib)