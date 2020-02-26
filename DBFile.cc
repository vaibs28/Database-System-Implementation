#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedDBFile.h"
#include "HeapDBFile.h"
#include <iostream>


//constructor initializing the required objects
DBFile::DBFile() {

}

//destructor
DBFile::~DBFile() {
}

//loads the DBFile instance from a textfile
int DBFile::Load(Schema &f_schema, char *loadpath) {
    genericDbFile->Load(f_schema, loadpath);
}

void DBFile::Add(Record &addMe) {
    genericDbFile->Add(addMe);
}

void DBFile::MoveFirst() {
    //move to the first page
    genericDbFile->MoveFirst();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type == heap) {
        genericDbFile = new HeapDBFile();
    } else if (f_type == sorted) {
        genericDbFile = new SortedDBFile();
    }
    genericDbFile->Create(f_path, f_type, startup);
}


int DBFile::Open(const char *f_path) {
    genericDbFile->Open(f_path);
}


int DBFile::Close() {
    genericDbFile->Close();
}


int DBFile::GetNext(Record &fetchme) {
    genericDbFile->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    genericDbFile->GetNext(fetchme, cnf, literal);
}


