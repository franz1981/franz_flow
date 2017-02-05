cmake_minimum_required(VERSION 3.6)
project(franz_flow)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -lrt -lpthread -std=gnu11")
set(SOURCE_FILES main.c record_descriptor.h index.h ring_buffer.h bytes_utils.h)
add_executable(franz_flow ${SOURCE_FILES})