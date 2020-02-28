//
// Created by Vaibhav Sahay on 25/02/20.
//

#include "SortedDBFile.h"
#include <iostream>
#include <sstream>
#define BUFFER_SIZE 100

OrderMaker *myOrder;
int runLength;

void SortedDBFile::MergeFileDataWithQueueData() {
    input->ShutDown();
    Record* removedRecord;
    while(output->Remove(removedRecord)){

    }
}

int SortedDBFile::Create(const char *f_path, fType file_type, void *startup) {
    file.Open(0, f_path);   // passing 0 as the first argument to create a new file at the given path
    endOfFile = 1;
    SortInfo *sortInfo;
    sortInfo = (SortInfo *) startup;
    runLength = sortInfo->runLength;
    myOrder = sortInfo->myOrder;
    cout << "file created successfully";
    return 1;
}


void SortedDBFile::MergeFileDataWithQueueData() {

}

int SortedDBFile::Load(Schema &myschema, char *loadpath) {
    Record *record;
    FILE *fd = fopen(loadpath, "r");
    while (record->SuckNextRecord(&myschema, fd)) {
        Add(*record);
    }
}


void SortedDBFile::Add(Record &addme) {
    if (mode == Writing) {
        input->Insert(&addme);
    } else if (mode == Reading) {
        input = new Pipe(BUFFER_SIZE);
        output = new Pipe(BUFFER_SIZE);
        bigQinstance = new BigQ(reinterpret_cast<Pipe &>(input), reinterpret_cast<Pipe &>(output),
                                reinterpret_cast<OrderMaker &>(myOrder), runLength);
        mode = Writing;
    }
}

void SortedDBFile::MoveFirst(){
    if(mode==Writing) {
        MergeFileDataWithQueueData();
        mode = Reading;
    }else{
        pageOffset = 0;
    }

}

int SortedDBFile::Close() {
    if(mode==Writing) {
        MergeFileDataWithQueueData();
        mode = Reading;
    }else{
        pageOffset = 0;         //resetting the page offset to start
        endOfFile = 1;
        return file.Close();
    }
}

int SortedDBFile::GetNext(Record &fetchme) {
    if(mode==Writing) {
        MergeFileDataWithQueueData();
        mode = Reading;
    }else{
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
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if(mode==Writing) {
        MergeFileDataWithQueueData();
        mode = Reading;
    }else {
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
}


int SortedDBFile::Open(const char *fpath) {

}

int SortedDBFile::readMetaData(ifstream &ifs) {
    string line;
    if (ifs.is_open()) {
        getline(ifs, line);
        getline(ifs, line);
        std::stringstream s_str(line);
        s_str >> runLength;
        myOrder->PutFromFile(ifs);
    }
}