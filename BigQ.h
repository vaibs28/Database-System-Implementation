#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <queue>
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <algorithm>
#include <ctime>
#include "Pipe.h"
#include "File.h"
#include "Record.h"


using namespace std;

typedef struct {
    Pipe *inPipe;
    Pipe *outPipe;
    OrderMaker *order;
    int runlen;
    string name;
}bigq_util;

class CompareTwoRecords {
    OrderMaker *order;
public:
    CompareTwoRecords(OrderMaker *order);
    bool operator() ( Record *left, Record  *right) const;
};


class ComparePQ {
    OrderMaker *order;
public:
    ComparePQ(OrderMaker *order);
    bool operator() ( RunRecord*  left, RunRecord* right) const;
};

typedef priority_queue<RunRecord* , vector<RunRecord*> ,ComparePQ> PQ;

class BigQ {

    File f;                  // File instance to hold pages
    Record rec;             // Record instance
    Page currentPage;      // Pointer to page to store the reference to the page instance

public:
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, string name);
    ~BigQ();

};
#endif
