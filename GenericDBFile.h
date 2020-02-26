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
    GenericDBFile();

    ~GenericDBFile();

    virtual int Create(const char *fpath, fType file_type, void *startup);

    virtual int Open(const char *fpath);

    virtual int Close();

    virtual int Load(Schema &myschema, char *loadpath);

    virtual void MoveFirst();

    virtual void Add(Record &addme);

    virtual int GetNext(Record &fetchme);

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif //A2TEST_GENERICDBFILE_H
