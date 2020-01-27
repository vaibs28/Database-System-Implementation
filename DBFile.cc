#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include <iostream>

// stub file .. replace it with your own DBFile.cc

//TODO:optimise for branch misses

//constructor initializing the required objects
DBFile::DBFile() {
}

//destructor
DBFile::~DBFile() {
}

//loads the DBFile instance from a textfile
void DBFile::Load(Schema &f_schema, char *loadpath) {
    FILE *fd = fopen(loadpath, "r");
    Record record;
    Page page;
    File file1;

    while (record.SuckNextRecord(&f_schema, fd)) {
        //append the record to page
        //recordCount++;
        current = &record; //store the address in the current pointer of DBFile
        if (page.Append(&record) == 0) {
            // cannot fit the new record, so add the existing page, clear the page and append the record
            file1.AddPage(&page, pageOffset++);
            page.EmptyItOut();
            page.Append(&record);
        }
    }
    file1.AddPage(&page, pageOffset++);
    file = &file1;  // store in the file pointer in DBFile
    cout << "Added to file" << endl;
    //cout<<"number of records="<<recordCount<<endl;
    fclose(fd); //close the opened file
    return;
}

void DBFile::Add(Record &addMe) {
    //append the record to the end of the page.
    Page page;
    File file;
    file.GetPage(&page,pageOffset);
    if (page.Append(&addMe) == 0) {
        // if the size is greater than the available page size then add a new page and clear it
        file.AddPage(&page, pageOffset++);
        page.EmptyItOut();
        page.Append(&addMe);
    }
    //add the page to the file
    file.AddPage(&page, pageOffset);
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


