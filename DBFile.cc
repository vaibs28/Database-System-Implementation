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

    this->file = new File();
    this->readPage = new Page();
    this->writePage = new Page();
    this->current = new Record();

}

//destructor
DBFile::~DBFile() {
    delete file;
    delete readPage;
    delete writePage;
    delete current;
}

//loads the DBFile instance from a textfile
void DBFile::Load(Schema &f_schema, char *loadpath) {

    FILE* tableFile = fopen (loadpath,"r");
    Record temp;

    while(temp.SuckNextRecord(&f_schema,tableFile)!=0)
        this->Add(temp);

    fclose(tableFile);
}

void DBFile::Add(Record &addMe) {
    if(!writePage->Append(&addMe)){
        file->AddPage(writePage,0);
        writePage->EmptyItOut();
        writePage->Append(&addMe);
    }
    file->AddPage(writePage,0);
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