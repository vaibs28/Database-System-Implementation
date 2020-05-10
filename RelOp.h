#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "BigQ.h"
#include <fstream>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include <string.h>
#include <sstream>

static char *SUM_ATT_NAME = "SUM";
static Attribute doubleAtt = {SUM_ATT_NAME, Double};
static Schema sumSchema("sum_schema", 1, &doubleAtt);

class RelationalOp {
public:
    // blocks the caller until the particular relational operator
    // has run to completion
    virtual void WaitUntilDone() = 0;

    // tell us how much internal memory the operation can use
    virtual void Use_n_Pages(int n) = 0;
};

class SelectPipe : public RelationalOp {

private:
    Pipe *input;
    Pipe *output;
    CNF *cnf;
    Record *rec;
    int runlen;
    pthread_t myThread;


public:
    SelectPipe() {};

    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~SelectPipe() {};
};

class SelectFile : public RelationalOp {
private:
    DBFile *dbfile;
    Pipe *output;
    CNF *cnf;
    Record *rec;
    int runlen;
    pthread_t myThread;

public:

    SelectFile() {};

    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~SelectFile() {};

};


class Project : public RelationalOp {
private:
    Pipe *input;
    Pipe *output;
    int *keepAtts;
    int noAttsIn;
    int noAttsOut;
    int runlen;
    pthread_t myThread;


public:
    Project() {};

    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~Project() {};
};

class WriteOut : public RelationalOp {
private:
    Pipe *input;
    FILE *file;
    Schema *schema;
    int runlen;
    pthread_t myThread;

public:
    WriteOut() {};

    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~WriteOut() {};
};

class DuplicateRemoval : public RelationalOp {
private:
    Pipe *input;
    Pipe *output;
    Schema *schema;
    int runlen;
    pthread_t myThread;

public:
    DuplicateRemoval() {};

    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~DuplicateRemoval() {};
};

class Sum : public RelationalOp {
private:
    Pipe *input;
    Pipe *output;
    Function *compute;
    int distinct;
    pthread_t myThread;
    int runlen;

public:
    Sum() {};

    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe,int distinct);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~Sum() {};
};

class GroupBy : public RelationalOp {
private:
    Pipe *input;
    Pipe *output;
    OrderMaker *myorder;
    Function *compute;
    int distinctFunc;
    int runlen;
    pthread_t myThread;

public:
    GroupBy() {};

    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, int distinctFunc);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~GroupBy() {};
};

class Join : public RelationalOp {
private:
    Pipe *inLeft;
    Pipe *inRight;
    Pipe *output;
    CNF *myCNF;
    Record *rec;
    int runlen;
    pthread_t myThread;

public:
    Join() {};

    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);

    void WaitUntilDone();

    void Use_n_Pages(int n);

    void StartOperation();

    ~Join() {};
};

#endif