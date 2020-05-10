#include "gtest/gtest.h"
#include <assert.h>
#include <iostream>
#include <map>
#include <string.h>
#include <vector>
#include "../QueryPlanner.h"
#include "../QueryNode.h"
#include "../Statistics.h"

#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

using namespace std;
extern "C" {
int yyparse(void);
}

extern struct TableList *tables;
extern struct FuncOperator *finalFunction;
extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern int distinctFunc;
extern int distinctAtts;
extern struct NameList *attsToSelect;


vector <AndList> selectList;
vector <AndList> joinList;
vector<AndList> join;
Statistics *stats = new Statistics();

QueryPlanner qp;
QueryNode qt;

/**
 *  All tests specific to q6
 *  SELECT SUM (ps.ps_supplycost), s.s_suppkey
    FROM part AS p, supplier AS s, partsupp AS ps
    WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0)
    GROUP BY s.s_suppkey

 */
TEST(QueryTest, Test1) {
    GTEST_COUT<<"Check If Stats has been Initialized Successfully"<<endl;
    int expectedVal = 1;
    Statistics *stats = new Statistics();
    int retVal = qp.initStats(stats);
    ASSERT_EQ(expectedVal,retVal);
}

TEST(QueryTest, TestGetOpName) {
    GTEST_COUT<<"Check If Operation Returns Correct Name"<<endl;
    string expectedVal = "SELECT FILE";
    QueryNodeType qnt=SELECTFILE;
    qt.type = qnt;
    string actualVal = qt.GetOpName();
    //Statistics *stats = new Statistics();
    //int retVal = qp.initStats(stats);
    ASSERT_EQ(expectedVal,actualVal);
}

TEST(QueryTest, NumberOfJoinsSuccess) {
    GTEST_COUT<<"Testing the number of joins in the entered query"<<endl;
    Statistics *stats = new Statistics();
    int retVal = qp.initStats(stats);
    yyparse();
    QueryPlanner qp;
    QueryNode qt;
    vector<string> rows;
    string initiate;
    qp.joinOperation(*stats, joinList, join, selectList);
    int numJoinActual = joinList.size();
    GTEST_COUT << "Returned value =" << numJoinActual<<endl;
    int numJoinExpected = 2;
    ASSERT_EQ(numJoinActual, numJoinExpected);
}

TEST(QueryTest, NumberOfJoinsFailure) {
    GTEST_COUT<<"Testing the number of joins in the entered query"<<endl;
    Statistics *stats = new Statistics();
    int retVal = qp.initStats(stats);
    yyparse();
    QueryPlanner qp;
    QueryNode qt;
    vector<string> rows;
    string initiate;
    qp.joinOperation(*stats, joinList, join, selectList);
    int numJoinActual = joinList.size();
    GTEST_COUT << "Returned value =" << numJoinActual<<endl;
    int numJoinExpected = 1;
    ASSERT_NE(numJoinActual, numJoinExpected);
}

TEST(QueryTest, NumberOfSelectsSuccess) {
    GTEST_COUT<<"Testing the number of selects query"<<endl;
    vector<string> rows;
    string initiate;
    int numSelectActual = selectList.size();
    GTEST_COUT << "Returned value =" << numSelectActual<<endl;
    int numSelectExpected = 1;
    ASSERT_EQ(numSelectActual, numSelectExpected);
}

TEST(QueryTest, NumberOfSelectsFailure) {
    GTEST_COUT<<"Testing the number of selects query"<<endl;
    vector<string> rows;
    string initiate;
    int numSelectActual = selectList.size();
    int numSelectExpected = 2;
    ASSERT_NE(numSelectActual, numSelectExpected);
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}