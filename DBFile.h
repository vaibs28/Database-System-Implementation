#ifndef DBFILE_H
#define DBFILE_H

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

class DBFile {

    Record *current;    // pointer to the current record in the file
    File file;          // instance of File class which will store pages
    Page page;          // a single page buffer which can store multiple records
    off_t pageOffset;   // index of the page being used

public:
    DBFile();

    ~DBFile();

    int Create(const char *fpath, fType file_type, void *startup);

    int Open(const char *fpath);

    int Close();

    void Load(Schema &myschema, char *loadpath);

    void MoveFirst();

    void Add(Record &addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

    File getFile() {
        return file;
    }

    off_t getPageOffset() {
        return pageOffset;
    }

    Page getPage() {
        return page;
    }
};

#endif
