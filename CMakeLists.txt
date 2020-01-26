cmake_minimum_required(VERSION 3.15)
project(P1)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(P1
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        DBFile.cc
        DBFile.h
        Defs.h
        File.cc
        File.h
        main.cc
        ParseTree.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        test.h
        TwoWayList.cc
        TwoWayList.h)
