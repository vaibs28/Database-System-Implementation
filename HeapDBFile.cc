//
// Created by Vaibhav Sahay on 25/02/20.
//

#include "HeapDBFile.h"
#include <iostream>

int HeapDBFile::Create(const char *f_path, fType file_type, void *startup) {
    file.Open(0, f_path);   // passing 0 as the first argument to create a new file at the given path
    //writeIsDirty = 0;
    endOfFile = 1;
    cout << "file created successfully : " << f_path << '\n';
    return 1;
}

int HeapDBFile::Open(const char *fpath) {
//mark the flag as true when open operation starts and mark it as false when the operation is over
    isBeingRead = true;
    file.Open(1, fpath);   //passing 1 as the first argument to open an already existing file
    isBeingRead = false;
    File f;
    if (f.getFileDes() > 0)
        return 1;
    return 0;
}

int HeapDBFile::GetNext(Record &fetchme) {
    if (endOfFile != 1) {
        if (!page.GetFirst(&fetchme)) {
            if (file.GetLength() <= pageOffset + 1)
                return 0;
            file.GetPage(&page, pageOffset++);
            page.GetFirst(&fetchme);
        }
        return 1;
    }
    return 0;
}

int HeapDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    int flag = 0;
    ComparisonEngine comp;
    while (GetNext(fetchme)) {
        if (comp.Compare(&fetchme, &literal, &cnf)) {
            flag = 1;
            break;
        }
    }
    if (flag == 0) {
        return 0;
    }
    return 1;
}

int HeapDBFile::Close() {
    pageOffset = 0;         //resetting the page offset to start
    endOfFile = 1;
    return file.Close();
}

int HeapDBFile::Close1() {
    if(page.getCurSizeInBytes()>0) {
        file.AddPage(&page, pageOffset++);
        page.EmptyItOut();
    }
    pageOffset = 0;         //resetting the page offset to start
    endOfFile = 1;
    return file.Close();
}

int HeapDBFile::Load(Schema &f_schema, char *loadpath) {
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

void HeapDBFile::Add(Record &addMe) {
    if (page.Append(&addMe) == 0) {
        file.AddPage(&page, pageOffset++);
        page.EmptyItOut();
        page.Append(&addMe);
    }
}

void HeapDBFile::MoveFirst() {
    pageOffset = 0;
}

int HeapDBFile::GetLength(){
    return file.GetLength();
}
