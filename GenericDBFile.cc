//
// Created by Vaibhav Sahay on 25/02/20.
//
#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

//initializing the static variables to false. Will be used in load method to check if the page is being read by other users.
bool GenericDBFile::isBeingRead = false;

GenericDBFile::GenericDBFile() {
    pageOffset = 0;
}

int GenericDBFile::Create(const char *f_path, fType f_type, void *startup) {
}

void GenericDBFile::Add(Record &addMe) {
    if (page.Append(&addMe) == 0) {
        page.EmptyItOut();
        page.Append(&addMe);
    }
    return;
}

void GenericDBFile::MoveFirst() {
    pageOffset = 0;
}

int GenericDBFile::Load(Schema &f_schema, char *loadpath) {
    FILE *fd = fopen(loadpath, "r");    // opens the file and returns the file descriptor
    if (fd == NULL) {
        return -1;
    }
    Record record;
    int numRecords = 0;
    while (record.SuckNextRecord(&f_schema, fd)) {  // while next record exists
        current = &record;                          //store the address in the current pointer of DBFile
        numRecords++;                               // increment the number of records
        int returnVal = page.Append(&record);       //add the record to the Page instance
        if (isBeingRead) {                           // add the Page instance to the file if the File is being read, to prevent dirty read
            cout << "File being read by a different user" << endl;
            file.AddPage(&page, pageOffset++);
            page.EmptyItOut();                      // clear the page instance and use the same instance for the next record
        }
        if (returnVal == 0) {
            // cannot fit the new record, so add the existing page to the File instance , clear the page and append the current record
            file.AddPage(&page, pageOffset++);
            page.EmptyItOut();
            page.Append(&record);
        }
    }
    if (!isBeingRead)
        file.AddPage(&page, pageOffset++);
    cout << numRecords << " records Added to file" << endl;
    return fclose(fd);                              //close the opened file
}

int GenericDBFile::Open(const char *f_path) {
    //mark the flag as true when open operation starts and mark it as false when the operation is over
    isBeingRead = true;
    file.Open(1, f_path);   //passing 1 as the first argument to open an already existing file
    isBeingRead = false;
    File f;
    if (f.getFileDes() > 0)
        return 1;
    return 0;
}

int GenericDBFile::Close() {
}

int GenericDBFile::GetNext(Record &fetchme) {
}

int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

}