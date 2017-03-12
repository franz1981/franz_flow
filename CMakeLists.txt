cmake_minimum_required(VERSION 3.6)
project(franz_flow)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -lrt -lpthread -std=gnu11")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)

SET(SOURCE
    src/bytes_utils.c
        src/fs_rb.c
        src/fs_stream.c
    src/vs_rb.c)

SET(HEADERS
        include/fs_rb.h
    include/index.h
        include/fs_stream.h
    include/vs_rb.h)

include_directories("src")
include_directories("include")

add_library(franz_flow ${SOURCE} ${HEADERS})
add_executable(main_fixed_size_rb test/main_fixed_size_rb.c)
add_executable(main_rb test/main_rb.c)
add_executable(main_stream test/main_stream.c test/main_stream.c)
add_executable(shared_rb_read test/shared_rb_read.c test/shared_rb_read.c test/shared_rb_read.c)
add_executable(shared_rb_write test/shared_rb_write.c test/shared_rb_write.c test/shared_rb_write.c test/shared_rb_write.c)