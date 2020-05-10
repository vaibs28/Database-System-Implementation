//#ifndef QUERY_TREE_NODE_H
//#define QUERY_TREE_NODE_H

//#define P_SIZE 100

#include "Comparison.h"
#include "DBFile.h"
#include "Function.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "RelOp.h"
#include "Schema.h"
#include "SortedDBFile.h"
#include <vector>


enum QueryNodeType {
    SELECTFILE, SELECTPIPE, PROJECT, JOIN, SUM, GROUPBY, DISTINCT, WRITE
};

/**
 * class to represent a node of the tree
 */
class QueryNode {

public:

    //node types referring to RelOp
    SelectFile *sf;
    SelectPipe *sp;
    Join *join;
    GroupBy *grpby;
    Project *proj;
    Sum *sum;
    DuplicateRemoval *dr;
    WriteOut *wo;

    //node links and type
    QueryNodeType type;
    QueryNode *root;
    QueryNode *left;
    QueryNode *right;

    //node specific members
    //select pipe
    Pipe *input;
    Pipe *output;
    Record *rec;
    CNF *opCNF;
    char* tablename;

    //select file
    DBFile *db;

    //project
    int noAttsIn;
    int noAttsOut;
    int *atttributesToKeep;
    int numOfAttributesToKeep;

    //write out
    //Pipe *input;
    FILE *file;
    Schema *schema;

    //Duplicate removal
    //Pipe *input;
    //Pipe *output;
    //Schema *schema;

    //Sum
    Function *f;

    string path;
    int leftChildPipe;
    int rightChildPipe;
    int outputPipe;
    AndList* cnf;
    AndList* tempCNFForJoins;
    OrderMaker *order;
    FuncOperator *funcOp;
    int distinct;

    QueryNode();

    ~QueryNode();


    void PrintLeftPipe(int lPipe);

    void PrintRightPipe(int rPipe);

    void PrintOutPipe(int oPipe);

    void PrintSchema();

    void PrintNode();

    void PrintCNF();

    void PrintFunction();

    void SetType(QueryNodeType typeSet);

    void CreateSchema();

    void CreateFunction();

    void CreateOrderMaker(vector<int> whichAttributes, int numOfAttributes, vector<int> typesOfAttributes);

    void postOrder();

    void inOrder();

    string GetOpName();

    void setGrpBy(QueryNode *enterNode, QueryNode *highOrderNode);

    void setProject(QueryNode *enterNode, QueryNode *iterableNode, QueryNode *highOrderNode);

    void setAttr(QueryNode *enterNode, QueryNode *iterableNode);

    void
    setGrpAtt(QueryNode *enterNode, int numOfAttributes, vector<int> typesOfAttributes, vector<int> whichAttributes);

    void setVal(QueryNode *enterNode, QueryNode *leftTNode, QueryNode *rightTNode, QueryNode *highOrderNode);

    void attSet(QueryNode *iterableNode, QueryNode *enterNode, bool printFlag);

    void setSum(QueryNode *enterNode, QueryNode *highOrderNode);

    void setDistinct(QueryNode *enterNode, QueryNode *highOrderNode);

    void execute();

    void waitUntilDone();

};

//#endif
