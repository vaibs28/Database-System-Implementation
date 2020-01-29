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
    pageOffset = 0;
}

//destructor
DBFile::~DBFile() {
}

//loads the DBFile instance from a textfile
void DBFile::Load(Schema &f_schema, char *loadpath) {
    FILE *fd = fopen(loadpath, "r");
    Record record;
    int numRecords = 0;
    while (record.SuckNextRecord(&f_schema, fd)) {
        //append the record to page
        current = &record; //store the address in the current pointer of DBFile
        numRecords++;
        if (page.Append(&record) == 0) {
            // cannot fit the new record, so add the existing page, clear the page and append the record
            file.AddPage(&page, pageOffset++);
            page.EmptyItOut();
            page.Append(&record);
        }
    }
    file.AddPage(&page, pageOffset++);
    cout << numRecords << " records Added to file" << endl;
    fclose(fd); //close the opened file
    return;
}

void DBFile::Add(Record &addMe) {
    //append the record to the end of the page.
    if (!page.Append(&addMe)) {
        page.EmptyItOut();
        page.Append(&addMe);
    }
    return;
}

void DBFile::MoveFirst() {
    page.EmptyItOut();
    //move to the first one
    pageOffset = 0;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type == heap) {
        file.Open(0, f_path);
        cout << "file created successfully";
        return 1;
    }
    return 0;
}


int DBFile::Open(const char *f_path) {
    file.Open(1, f_path);
    return 1;
}


int DBFile::Close() {
    pageOffset = 0;
    return file.Close();
}


int DBFile::GetNext(Record &fetchme) {
    if (!page.GetFirst(&fetchme)) {
        if (pageOffset + 1 >= file.GetLength())
            return 0;
        file.GetPage(&page, pageOffset++);
        page.GetFirst(&fetchme);
    }
    return 1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    int find_flag = 0;
    ComparisonEngine comp;
    while (GetNext(fetchme)) {
        if (comp.Compare(&fetchme, &literal, &cnf)) {
            //fetchme.Print(&mySchema);
            find_flag = 1;
            break;
        }
    }

    if (find_flag == 0) {
        //no records found
        return 0;
    }
    return 1;
}


