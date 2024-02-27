cmake_minimum_required(VERSION 3.27)
project(matrix)

set(CMAKE_CXX_STANDARD 17)

add_executable(matrix main.cpp
        matrix.cpp
        matrix.h)
