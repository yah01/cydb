add_library(cyengine SHARED "")
SET_TARGET_PROPERTIES(cyengine PROPERTIES LINKER_LANGUAGE CXX)

find_library(ROCKSDB_LIB rocksdb)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
target_link_libraries(cyengine ${ROCKSDB_LIB})

target_sources(
    cyengine
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/engines
    )
include_directories(${CMAKE_CURRENT_LIST_DIR})
target_include_directories(
    cyengine
    PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}/engines
)