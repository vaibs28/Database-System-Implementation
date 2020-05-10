#ifndef STATISTICS_
#define STATISTICS_

#include "ParseTree.h"
#include <map>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

using namespace std;

/**
 * class to store the relation data, corresponding to the relation name in Statistics class
 */
class RelationData {
private:

    int numRows;
    map<string, int> attributeMap;
    string groupName;
    int groupSize;

public:

    RelationData(int n, string grpname) : numRows(n), groupName(grpname) {
        groupSize = 1;
    }

    RelationData(RelationData &copyMe) {

        numRows = copyMe.getTupleCount();
        map<string, int> *ptr = copyMe.getAttributes();
        map<string, int>::iterator itr;
        for (itr = ptr->begin(); itr != ptr->end(); itr++) {
            attributeMap[itr->first] = itr->second;
        }
        groupSize = copyMe.groupSize;
        groupName = copyMe.groupName;
    }

    ~RelationData() { attributeMap.clear(); }

    //Update container Data,Overloaded functions
    void setNumRows(int n) {
        numRows = n;
    }

    void setData(string s, int num_distinct) {
        attributeMap[s] = num_distinct;
    }

    map<string, int> *getAttributes() {
        return &attributeMap;
    }

    int getTupleCount() {
        return numRows;
    }

    string getGroupName() {
        return groupName;
    }

    int getGroupSize() {
        return groupSize;
    }

    void setGroupData(string grpname, int grpcnt) {
        groupName = grpname;
        groupSize = grpcnt;
    }
};

/*Container for Database Statistics*/
class Statistics {
private:
    map<string, RelationData *> dataMap;  //map for table name and table data
public:
    Statistics();

    Statistics(Statistics &copyMe);     // Performs deep copy
    ~Statistics();

    void AddRel(char *relName, int numTuples);

    void AddAtt(char *relName, char *attName, int numDistincts);

    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);

    void Write(char *fromWhere);

    bool Validate(struct AndList *parseTree, char *relNames[], int numToJoin, map<string, long> &uniqvallist);

    bool ContainsAttribute(char *value, char *relNames[], int numToJoin, map<string, long> &uniqvallist);

    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);

    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

    double Evaluate(struct OrList *orList, map<string, long> &uniqvallist);

    void applyHelper(struct AndList *parseTree, char *identifications[], int joinCount);
    void ParseRelation(struct Operand* op, string& relation);

    map<string, RelationData *> *GetDbStats() {
        return &dataMap;
    }

};

#endif