//
// Created by Vaibhav Sahay on 25/02/20.
//

#include "HeapDBFile.h"
#include <iostream>

int GenericDBFile::Create(const char *f_path, fType file_type, void *startup) {
    file.Open(0, f_path);   // passing 0 as the first argument to create a new file at the given path
    //writeIsDirty = 0;
    endOfFile = 1;
    cout << "file created successfully";
    return 1;
}

int GenericDBFile::Open(const char *fpath) {

}

int GenericDBFile::GetNext(Record &fetchme) {

}