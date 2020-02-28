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
}

void GenericDBFile::MoveFirst() {
}

int GenericDBFile::Load(Schema &f_schema, char *loadpath) {

}

int GenericDBFile::Open(const char *f_path) {
}

int GenericDBFile::Close() {
}

int GenericDBFile::GetNext(Record &fetchme) {
}

int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
}