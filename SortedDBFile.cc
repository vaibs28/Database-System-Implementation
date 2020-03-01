//
// Created by Vaibhav Sahay on 25/02/20.
//

#include "SortedDBFile.h"
#include "HeapDBFile.h"
#include <iostream>
#include <sstream>
#define BUFFER_SIZE 100

OrderMaker *myOrder;
int runLength;
Schema *orderMakerSchema;

SortedDBFile::SortedDBFile(){

}

int SortedDBFile::Create(const char *f_path, fType file_type, void *startup) {
    file.Open(0, f_path);   // passing 0 as the first argument to create a new file at the given path
    endOfFile = 1;
    SortInfo *sortInfo;
    sortInfo = (SortInfo *) startup;
    runLength = sortInfo->runLength;
    myOrder = sortInfo->myOrder;
    cout << "sorted file created successfully"<<endl;
    return 1;
}

int SortedDBFile::Load(Schema &myschema, char *loadpath) {
    orderMakerSchema = &myschema;
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
        input = new Pipe(BUFFER_SIZE);      // create new instance of the pipes
        output = new Pipe(BUFFER_SIZE);
        bigQinstance = new BigQ(reinterpret_cast<Pipe &>(input), reinterpret_cast<Pipe &>(output),
                                reinterpret_cast<OrderMaker &>(myOrder), runLength);
        input->Insert(&addme);
        mode = Writing;
    } else {
        cout << "error" <<endl;
    }
}

void SortedDBFile::MergeFileDataWithQueueData() {
    //shutdown input pipe
    input->ShutDown();
    //remove the records from output pipe one by one and check with the sorted file to merge
    ComparisonEngine ce;
    Record recordFromOutputPipe;
    Record recordFromFile;
    int i = 0;
    //tempfile to store sorted segments while merging
    HeapDBFile *tempFile = new HeapDBFile();
    char *a = "tmp";
    tempFile->Create(a, heap, NULL);
    //get the first records from both
    int fromFile = GetNext(recordFromFile);
    int fromPipe = output->Remove(&recordFromOutputPipe);
    while(true) {
        //get the records from sorted file and compare with recordFromOutputPipe
        if(fromFile && fromPipe) {   // if both have records
            int retVal = ce.Compare(&recordFromFile, &recordFromOutputPipe, myOrder);
            if (retVal == -1 || retVal == 0) { // recordfromFile is lesser
                //add to the tempfile
                tempFile->Add(recordFromFile);
                fromFile = GetNext(recordFromFile);
            } else { // recordFromFile is greater
                tempFile->Add(recordFromOutputPipe);
                fromPipe = output->Remove(&recordFromOutputPipe);
            }
        }else{
            //no records to compare
            break;
        }
    }// while loop ends
    //add the remaining records from file and the pipe
    while(fromFile){
        tempFile->Add(recordFromFile);
        fromFile = GetNext(recordFromFile);
    }

    while(fromPipe){
        tempFile->Add(recordFromOutputPipe);
        fromPipe = output->Remove(&recordFromOutputPipe);
    }

    tempFile->Close();
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
        examineCNF(cnf);
    }
}

void SortedDBFile::examineCNF(CNF &cnf) {
    //OrderMaker orderMakerFromCNF;
    Attribute* atts =  orderMakerSchema->GetAtts();

}

int SortedDBFile::Open(const char *fpath) {
    isBeingRead = true;
    file.Open(1, fpath);   //passing 1 as the first argument to open an already existing file
    isBeingRead = false;
    //readMetaData(fpath);
    return 0;
}

int SortedDBFile::readMetaData(const char *fpath) {
    string metafileName;
    int type;
    metafileName.append(fpath);
    metafileName.append(".meta");
    ifstream metafile;
    metafile.open(metafileName.c_str());
    if(!metafile)
        return -1;
    metafile >> type;
    fType dbfileType = (fType) type;
    metafile.close();
}


