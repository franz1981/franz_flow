cmake_minimum_required(VERSION 3.6)
project(franz_flow)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -lrt -lpthread -std=gnu11")
set(SOURCE_FILES main_rb.c message_layout.h index.h ring_buffer.h bytes_utils.h ring_buffer_layout.h fixed_size_ring_buffer.c fixed_size_ring_buffer.h main_ff_spsc.c)
add_executable(franz_flow ${SOURCE_FILES})