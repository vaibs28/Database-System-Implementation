#ifndef BIGQ_H
#define BIGQ_H

#include <algorithm>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include <math.h>
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include <vector>
#include <map>

using namespace std;

class ComparisonEngine;


class BigQ {

    File f;                  // File instance to hold pages
    Record rec;             // Record instance
    Page *currentPage;      // Pointer to page to store the reference to the page instance

public:
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);

    ~BigQ();

    int generateRuns(OrderMaker &sortorder);

    void mergeRuns();

    void writeRunsToFile(int runlen);

    int createTempFile();
};


#endif



