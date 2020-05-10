#include "Comparison.h"
#include "Function.h"
#include "QueryNode.h"
#include "Schema.h"
#include <iostream>
#include <vector>

using namespace std;

/**
 * Utility functions to print each node type
 */

extern struct FuncOperator *finalFunction;
extern struct NameList *attsToSelect;

QueryNode::QueryNode() {
    leftChildPipe = 0;
    rightChildPipe = 0;
    outputPipe = 0;
    sf = NULL;
    sp = NULL;
    join = NULL;
    grpby = NULL;
    proj = NULL;
    sum = NULL;
    dr = NULL;
    wo = NULL;
    db = NULL;
    cnf = NULL;
    f = NULL;
    funcOp = NULL;
    opCNF = NULL;
    order = NULL;
    schema = NULL;
    left = NULL;
    right = NULL;
    root = NULL;
    atttributesToKeep = NULL;
    numOfAttributesToKeep = 0;
}

QueryNode::~QueryNode() {}

void QueryNode::SetType(QueryNodeType typeSet) {
    type = typeSet;
}

void QueryNode::inOrder() {
    if (NULL != left) {
        left->inOrder();
    }
    PrintNode();
    if (NULL != right) {
        right->inOrder();
    }
}

void QueryNode::postOrder() {
    if (NULL != left) {
        left->postOrder();
    }
    if (NULL != right) {
        right->postOrder();
    }
    PrintNode();
}

std::string QueryNode::GetOpName() {
    string name;
    const char *opNames[] = {"SELECT FILE", "SELECT PIPE", "PROJECT", "JOIN", "SUM", "GROUP BY", "DISTINCT", "WRITE"};
    return opNames[type];
}


void QueryNode::PrintLeftPipe(int lPipe) {
    cout << "Left Input Pipe " << lPipe << endl;
}

void QueryNode::PrintRightPipe(int rPipe) {
    cout << "Right Input Pipe " << rPipe << endl;
}

void QueryNode::PrintOutPipe(int oPipe) {
    cout << "Output Pipe " << oPipe << endl;
}

void QueryNode::PrintSchema() {
    cout << "Output Schema: " << endl;
    schema->Print();
}

void QueryNode::PrintNode() {
    cout << " *********** " << endl;
    cout << GetOpName() << " operation" << endl;
    if (type == SELECTFILE) {
        cout << "Input Pipe " << leftChildPipe << endl;
        PrintOutPipe(outputPipe);
        PrintSchema();
        PrintCNF();
    } else if (type == SELECTPIPE) {
        cout << "Input Pipe " << leftChildPipe << endl;
        PrintOutPipe(outputPipe);
        PrintSchema();
        cout << "SELECTION CNF :" << endl;
        PrintCNF();
    } else if (type == JOIN) {
        PrintLeftPipe(leftChildPipe);
        PrintRightPipe(rightChildPipe);
        PrintOutPipe(outputPipe);
        PrintSchema();
        cout << endl << "CNF: " << endl;
        PrintCNF();
    } else if (type == PROJECT) {
        cout << "Input Pipe " << leftChildPipe << endl;
        PrintOutPipe(outputPipe);
        PrintSchema();
        cout << endl << "************" << endl;
    } else if (type == GROUPBY) {
        PrintLeftPipe(leftChildPipe);
        PrintOutPipe(outputPipe);
        PrintSchema();
        cout << endl << "GROUPING ON " << endl;
        order->Print();
        cout << endl << "FUNCTION " << endl;
        PrintFunction();
    } else if (type == SUM) {
        PrintLeftPipe(leftChildPipe);
        PrintOutPipe(outputPipe);
        PrintSchema();
        cout << endl << "FUNCTION: " << endl;
        PrintFunction();
    } else if (type == WRITE) {
        PrintLeftPipe(leftChildPipe);
        cout << "Output File " << path << endl;
    } else if (type == DISTINCT) {
        PrintLeftPipe(leftChildPipe);
        PrintOutPipe(outputPipe);
        PrintSchema();
        cout << endl << "FUNCTION: " << endl;
        PrintFunction();
    }
}

void QueryNode::execute() {

    if (type == SELECTFILE) {
        db->Open(strcat(this->tablename, ".bin"));
        sf->Use_n_Pages(10);
        sf->Run(*db, *output, *opCNF, *rec);
        sf->WaitUntilDone();
    } else if (type == SELECTPIPE) {
        return;
        db = new DBFile();
        db->Open(this->left->tablename);
        sp = new SelectPipe();
        //sp->Use_n_Pages(10);
        input = new Pipe(100);
        Record fetchme;
        while (db->GetNext(fetchme)) {
            input->Insert(&fetchme);
        }
        output = new Pipe(100);
        sp->Run(*input, *output, *opCNF, *rec);
    } else if (type == JOIN) {
        join = new Join();
        join->Use_n_Pages(10);
        if(left->type==SELECTPIPE){
            left = left->left;
        }
        if(right->type==SELECTPIPE){
            right = right->left;
        }
        output = new Pipe(100);
        join->Run(*(left->output), *(right->output), *output, *opCNF, *rec);
        //join->WaitUntilDone();
    } else if (type == PROJECT) {
        proj = new Project();
        db = new DBFile();
        //get to the selectfile
        QueryNode* temp = this;
        while(temp->type!=SELECTFILE){
            temp = temp->left;
        }
        db->Open(temp->tablename);
        input = new Pipe(100);
        output = new Pipe(100);
        Record fetchme;
        proj->Use_n_Pages(10);
        while(true){
            if(left->type==JOIN || left->type==SELECTFILE){
                break;
            }
            left = left->left;
        }
        proj->Run(*left->output, *output, atttributesToKeep, noAttsIn, noAttsOut);

    } else if (type == GROUPBY) {
        grpby = new GroupBy();
        grpby->Use_n_Pages(10);
        output = new Pipe(100);
        if(left->type==DISTINCT){
            left = left->left;
        }
        grpby->Run(*left->output, *output, *order, *f,distinct);
        //grpby->WaitUntilDone();
    } else if (type == SUM) {
        sum = new Sum();
        //sum->Use_n_Pages(10);
        output = new Pipe(100);
        QueryNode* temp = this;
        sum->Run(*left->output, *output, *f , distinct);
        //sum->WaitUntilDone();
    } else if (type == WRITE) {
        wo = new WriteOut();
        wo->Use_n_Pages(100);
        wo->Run(*input, file, *schema);
    } else if (type == DISTINCT) {
        return;
        dr = new DuplicateRemoval();
        dr->Use_n_Pages(100);
        output = new Pipe(100);
        dr->Run(*left->output, *output, *schema);
        //dr->WaitUntilDone();
    }
}


void QueryNode::PrintCNF() {

    if (cnf) {

        struct OrList *currentOr;
        struct AndList *currentAnd = cnf;
        struct ComparisonOp *currentOperator;
        int currentOperatorCode;

        while (currentAnd) {
            currentOr = currentAnd->left;
            if (currentAnd->left) {
                cout << "(";
            }

            while (currentOr) {
                currentOperator = currentOr->left;
                if (currentOperator) {
                    if (currentOperator->left) {
                        cout << currentOperator->left->value;
                    }
                    currentOperatorCode = currentOperator->code;
                    if (currentOperatorCode == 5)
                        cout << " < ";
                    else if (currentOperatorCode == 6)
                        cout << " > ";
                    else if (currentOperatorCode == 7)
                        cout << " = ";

                    if (currentOperator->right) {
                        cout << currentOperator->right->value;
                    }
                }

                if (currentOr->rightOr) {
                    cout << " OR ";
                }
                currentOr = currentOr->rightOr;
            }

            if (currentAnd->left) {
                cout << ")";
            }

            if (currentAnd->rightAnd) {
                cout << " AND ";
            }
            currentAnd = currentAnd->rightAnd;
        }

    }
    cout << endl;
}

void QueryNode::PrintFunction() {
    f->Print(finalFunction, *schema);
}

void QueryNode::CreateSchema() {
    Schema *leftChildSchema = left->schema;
    Schema *rightChildSchema = right->schema;
    schema = new Schema(leftChildSchema, rightChildSchema);
}

void QueryNode::CreateFunction() {
    f = new Function();
    f->GrowFromParseTree(funcOp, *schema);
}

void QueryNode::CreateOrderMaker(vector<int> whichAttributes, int numOfAttributes, vector<int> typesOfAttributes) {

    order = new OrderMaker();
    order->numAtts = numOfAttributes;
    int i = 0;
    while (i < whichAttributes.size()) {
        order->whichAtts[i] = whichAttributes[i];
        order->whichTypes[i] = (Type) typesOfAttributes[i];
        i++;
    }
}


void QueryNode::setAttr(QueryNode *rootNode, QueryNode *iterableNode) {
    iterableNode->root = rootNode;
    rootNode->left = iterableNode;
    rootNode->schema = iterableNode->schema;
    rootNode->type = SELECTPIPE;
}

void QueryNode::setGrpAtt(QueryNode *rootNode, int numOfAttributes, vector<int> typesOfAttributes,
                          vector<int> whichAttributes) {

    rootNode->CreateOrderMaker(whichAttributes, numOfAttributes, typesOfAttributes);
    rootNode->funcOp = finalFunction;
    rootNode->CreateFunction();
}

void QueryNode::setVal(QueryNode *rootNode, QueryNode *left, QueryNode *right, QueryNode *highOrderNode) {
    rootNode->left = left;
    rootNode->right = right;
    left->root = rootNode;
    right->root = rootNode;
}

void QueryNode::attSet(QueryNode *iterableNode, QueryNode *rootNode,bool printFlag) {
    vector<int> attsIndex;
    Schema *oSchema = iterableNode->schema;
    NameList *iterateAtts = attsToSelect;
    string attb;
    while (iterateAtts != 0) {
        attb = iterateAtts->name;
        attsIndex.push_back(oSchema->Find(const_cast<char *>(attb.c_str())));
        iterateAtts = iterateAtts->next;
    }
    Schema *nSchema = new Schema(oSchema, attsIndex);
    rootNode->schema = nSchema;
    rootNode->noAttsOut = attsIndex.size();
    if(printFlag)
    rootNode->schema->Print();
}

void QueryNode::setGrpBy(QueryNode *rootNode, QueryNode *highOrderNode) {
    rootNode->type = GROUPBY;
    rootNode->left = highOrderNode;
    highOrderNode->root = rootNode;
    rootNode->leftChildPipe = highOrderNode->outputPipe;
}

void QueryNode::setProject(QueryNode *rootNode, QueryNode *iterableNode, QueryNode *highOrderNode) {
    rootNode->type = PROJECT;
    rootNode->left = iterableNode;
    iterableNode->root = rootNode;
    rootNode->leftChildPipe = iterableNode->outputPipe;
}

void QueryNode::setSum(QueryNode *rootNode, QueryNode *highOrderNode) {
    rootNode->type = SUM;
    rootNode->left = highOrderNode;
    highOrderNode->root = rootNode;
    rootNode->leftChildPipe = highOrderNode->outputPipe;
}

void QueryNode::setDistinct(QueryNode *rootNode, QueryNode *highOrderNode) {
    //rootNode = new QueryNode();
    rootNode->type = DISTINCT;
    rootNode->left = highOrderNode;
    highOrderNode->root = rootNode;
    rootNode->leftChildPipe = highOrderNode->outputPipe;
}
