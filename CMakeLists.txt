cmake_minimum_required(VERSION 3.6)
project(franz_flow)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -lrt -lpthread -std=gnu11")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)

SET(SOURCE
    src/fixed_size_ring_buffer.c
    src/fixed_size_stream.c)

SET(HEADERS
    include/bytes_utils.h  
    include/fixed_size_ring_buffer.h
    include/index.h
    include/message_layout.h
    include/ring_buffer.h
    include/ring_buffer_layout.h
    include/fixed_size_stream.h)

include_directories("src")
include_directories("include")

add_library(franz_flow ${SOURCE} ${HEADERS})
add_executable(main_fixed_size_rb test/main_fixed_size_rb.c)
add_executable(main_rb test/main_rb.c)
add_executable(main_stream test/main_stream.c test/main_stream.c)
add_executable(shared_rb_read test/shared_rb_read.c test/shared_rb_read.c test/shared_rb_read.c)
add_executable(shared_rb_write test/shared_rb_write.c test/shared_rb_write.c test/shared_rb_write.c test/shared_rb_write.c)