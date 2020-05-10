//
// Created by Vaibhav Sahay on 25/02/20.
//

#ifndef A2TEST_SORTEDDBFILE_H
#define A2TEST_SORTEDDBFILE_H


#include <fstream>
#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include "HeapDBFile.h"

typedef enum {Reading, Writing} Mode;

typedef struct SortInfo {
    OrderMaker *myOrder;
    int runLength;
}SortInfo;

class SortedDBFile : public virtual GenericDBFile{

public:
    Mode mode;
    Pipe* input;
    Pipe* output;
    bigq_util * util;
    pthread_t bigQThread;
    bool queryBuilt;
    bool sortOrderExists;
    OrderMaker *query;

    SortedDBFile();

    ~SortedDBFile();

    int readMetaData (const char *fpath);

    void MergeFileDataWithQueueData();

    void MoveFirst();

    void Add(Record &addme);

    int Create(const char *f_path, fType file_type, void *startup);

    int Load(Schema &myschema, char *loadpath);

    int Open(const char *fpath);

    int Close();

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

    void examineCNF(CNF &cnf);

    void clearQueues();

    int BinarySearch(Record &fetchme, CNF &cnf, Record &literal);

    int FindFromCNF(Record &record, CNF &cnf, Record &record1);

    int FindFromSortOrder(Record &record, CNF &cnf, Record &record1);
};


#endif //A2TEST_SORTEDDBFILE_H
