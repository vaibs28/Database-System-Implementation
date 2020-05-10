//
// Created by Vaibhav Sahay on 25/02/20.
//

#ifndef A2TEST_GENERICDBFILE_H
#define A2TEST_GENERICDBFILE_H


#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {
    heap, sorted, tree
} fType;

// stub DBFile header..replace it with your own DBFile.h

class GenericDBFile {

protected:
    Record *current;    // pointer to the current record in the file
    File file;          // instance of File class which will store pages
    Page page;          // a single page buffer which can store multiple records
    off_t pageOffset;   // index of the page being used
    int writeIsDirty;
    int endOfFile;
    static bool isBeingRead;   // flag to check if the file is being read

public:
    GenericDBFile(){};

    ~GenericDBFile(){};

    virtual int Create(const char *fpath, fType file_type, void *startup) = 0;

    virtual int Open(const char *fpath) = 0;

    virtual int Close() = 0;

    virtual int Load(Schema &myschema, char *loadpath) = 0;

    virtual void MoveFirst() = 0;

    virtual void Add(Record &addme) = 0;

    virtual int GetNext(Record &fetchme) = 0;

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

    int GetLength();
};

#endif //A2TEST_GENERICDBFILE_H
