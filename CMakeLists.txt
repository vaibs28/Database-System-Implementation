cmake_minimum_required(VERSION 3.15)
project(a3test)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(./temp/)
configure_file(catalog ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
configure_file(test.cat ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
add_executable(main
        main.cc
        Parser.y
        lex.yy.c
        y.tab.o
        ParseTree.h
        Record.h
        Function.h
        Function.cc
        Record.cc
        Comparison.h
        Comparison.cc
        #Compiler.h
        #Compiler.cpp
        ComparisonEngine.h
        ComparisonEngine.cc
        #CompilerInterfaces.h
        File.h
        File.cc
        GenericDBFile.h
        GenericDBFile.cc
        DBFile.h
        DBFile.cc
        Statistics.h
        Statistics.cc
        HeapDBFile.h
        HeapDBFile.cc
        SortedDBFile.h
        SortedDBFile.cc
        BigQ.h
        BigQ.cc
        RelOp.h
        RelOp.cc
        TwoWayList.h
        TwoWayList.cc
        Pipe.h
        Pipe.cc
        Schema.h
        Schema.cc
        catalog
        QueryPlanner.cc
        QueryNode.cc
        Ddl.cc Ddl.h)
