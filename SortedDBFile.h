//
// Created by Vaibhav Sahay on 25/02/20.
//

#ifndef A2TEST_SORTEDDBFILE_H
#define A2TEST_SORTEDDBFILE_H


#include <fstream>
#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"

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
    BigQ* bigQinstance;

    SortedDBFile() {}

    ~SortedDBFile(){}

    int readMetaData (ifstream &ifs);

    void MergeFileDataWithQueueData();

    void MoveFirst();

    int AddToOutPipeFile(Record &rec);

    void FlushWritePage();

    void Add(Record &addme);

    int Create(const char *f_path, fType file_type, void *startup);

    int Load(Schema &myschema, char *loadpath);

    int Open(const char *fpath);

    int Close();

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};


#endif //A2TEST_SORTEDDBFILE_H