#ifndef QUERYPLANNER_H
#define QUERYPLANNER_H

#include <assert.h>
#include <iostream>
#include <map>
#include <string.h>
#include <vector>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryNode.h"

using namespace std;

class QueryPlanner{

public:
    vector<AndList> DetermineJoinOrder(Statistics* stats, vector<AndList> andList);
    void joinOp(Statistics stats, vector<AndList>& links, vector<AndList>& join, vector<AndList>& pick);

    void joinOperation(Statistics stats, vector<AndList> &links, vector<AndList> &join, vector<AndList> &pick);

    int initStats(Statistics *stats);

    QueryNode* generate(Statistics *stats,bool flag);

    void print(QueryNode* root);

    void execute(QueryNode *root, char *string1);
};

#endif