cmake_minimum_required(VERSION 3.6)
project(franz_flow)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -lrt -lpthread -std=gnu11")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)

SET(SOURCE
    src/fixed_size_ring_buffer.c
)

SET(HEADERS
    include/bytes_utils.h  
    include/fixed_size_ring_buffer.h
    include/index.h
    include/message_layout.h
    include/ring_buffer.h
    include/ring_buffer_layout.h)

include_directories("src")
include_directories("include")

add_library(franz_flow ${SOURCE} ${HEADERS})
add_executable(main_ff_spsc test/main_ff_spsc.c)
add_executable(main_rb test/main_rb.c)
