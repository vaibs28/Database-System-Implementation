#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

// stub DBFile header..replace it with your own DBFile.h

class DBFile {

protected:
    GenericDBFile *genericDbFile;

public:
    DBFile();

    ~DBFile();

    int Create(const char *fpath, fType file_type, void *startup);

    int Open(const char *fpath);

    int Close();

    int Load(Schema &myschema, char *loadpath);

    void MoveFirst();

    void Add(Record &addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};

#endif
