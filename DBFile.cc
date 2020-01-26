#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>

// stub file .. replace it with your own DBFile.cc

//TODO:optimise for branch misses

//constructor initializing the required objects
DBFile::DBFile() {

    //File instance which will hold the DBFile records
    //this->file = new File();
    //Page instance
    //this->page = new Page();
    //current pointer to the record
    //this->current = new Record();

}

//destructor
DBFile::~DBFile() {
    //delete file;
    //delete page;
    //delete current;
}

//loads the DBFile instance from a textfile
void DBFile::Load(Schema &f_schema, char *loadpath) {

    /*//open the file from the loadpath
    FILE *tableFile = fopen(loadpath, "r");
    Record temp;
    //read the record and add to the end of the DBFile
    while (temp.SuckNextRecord(&f_schema, tableFile) != 0)
        this->Add(temp);
    fclose(tableFile);*/
}

void DBFile::Add(Record &addMe) {
}

void DBFile::MoveFirst() {

}

int DBFile::Create(char *f_path, fType f_type, void *startup) {

}


int DBFile::Open(char *f_path) {
}


int DBFile::Close() {
}


int DBFile::GetNext(Record &fetchme) {
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
}