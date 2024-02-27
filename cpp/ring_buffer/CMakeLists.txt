cmake_minimum_required(VERSION 3.27)
project(ring_buffer)

set(CMAKE_CXX_STANDARD 17)

add_executable(ring_buffer main.cpp
        ringbuff.h)
