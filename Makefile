
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o SortedDBFile.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o QueryPlanner.o QueryNode.o y.tab.o lex.yy.o Ddl.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o SortedDBFile.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o QueryPlanner.o QueryNode.o y.tab.o lex.yy.o Ddl.o main.o -ll -lpthread

main.o : main.cc
	$(CC) -g -c main.cc

gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o  HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o gtest-all.o testing.o RelOp.o Function.o yyfunc.tab.o lex.yyfunc.o Statistics.o QueryPlanner.o QueryNode.o
	$(CC) -o gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o gtest-all.o testing.o RelOp.o Function.o yyfunc.tab.o lex.yyfunc.o Statistics.o QueryPlanner.o QueryNode.o -ll -lpthread

a42test.out:  Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o SortedDBFile.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o y.tab.o lex.yy.o QueryPlanner.o QueryNode.o test42.o
	$(CC) -o a42test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o HeapDBFile.o SortedDBFile.o BigQ.o DBFile.o Statistics.o Pipe.o RelOp.o Function.o y.tab.o lex.yy.o QueryPlanner.o QueryNode.o test42.o -ll -lpthread

a41test.out: Statistics.o y.tab.o  lex.yy.o  test5.o
	$(CC) -o a41test.out Statistics.o y.tab.o lex.yy.o test5.o -ll -lpthread

a3test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o  HeapDBFile.o SortedDBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test4.o
	$(CC) -o a3test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o HeapDBFile.o SortedDBFile.o Pipe.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test4.o -ll -lpthread

a22test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o  HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o test3.o
	$(CC) -o a22test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o test3.o -ll -lpthread

a21test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o  HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o test2.o
	$(CC) -o a21test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDBFile.o DBFile.o HeapDBFile.o SortedDBFile.o Pipe.o y.tab.o lex.yy.o test2.o -ll -lpthread
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o  HeapDBFile.o BigQ.o Pipe.o SortedDBFile.o y.tab.o lex.yy.o a2test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapDBFile.o BigQ.o Pipe.o SortedDBFile.o y.tab.o lex.yy.o a2test.o -ll -lpthread

testing.o: Tests/testing.cc
	$(CC) -g -c "Tests/testing.cc"

gtest-all.o: Tests/gtest/gtest-all.cc
	$(CC) -g -c "Tests/gtest/gtest-all.cc"


test42.o: test42.cc
	$(CC) -g -c test42.cc

test5.o: test5.cc
	$(CC) -g -c test5.cc

test4.o: test4.cc
	$(CC) -g -c test4.cc

test3.o: test3.cc
	$(CC) -g -c test3.cc

test2.o: test2.cc
	$(CC) -g -c test2.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Ddl.o: Ddl.cc
	$(CC) -g -c Ddl.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -g -c GenericDBFile.cc

HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c SortedDBFile.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

QueryNode.o: QueryNode.cc
	$(CC) -g -c QueryNode.cc

QueryPlanner.o: QueryPlanner.cc
	$(CC) -g -c QueryPlanner.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/" y.tab.c
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f *.bin
	rm -f *.meta
	rm -f *.tmp