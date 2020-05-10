//
// Created by Vaibhav Sahay on 25/02/20.
//

#ifndef A2TEST_HEAPDBFILE_H
#define A2TEST_HEAPDBFILE_H


#include "GenericDBFile.h"

class HeapDBFile : public virtual GenericDBFile {

public:

    HeapDBFile() {
        pageOffset = 0;
        isBeingRead = false;
    }

    ~HeapDBFile() {}

    int Create(const char *fpath, fType file_type, void *startup);

    int Open(const char *fpath);

    int Close();

    int Load(Schema &myschema, char *loadpath);

    void MoveFirst();

    void Add(Record &addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

    int GetLength();

    int Close1();

};


#endif //A2TEST_HEAPDBFILE_H
