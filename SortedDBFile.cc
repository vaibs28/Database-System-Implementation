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
//HeapDBFile *file = new HeapDBFile();
//char *a = "tmp";



SortedDBFile::SortedDBFile(){
    //tempFile->Create(a, heap, NULL);
}

int SortedDBFile::Create(const char *f_path, fType file_type, void *startup) {
    file.Open(0, f_path);   // passing 0 as the first argument to create a new file at the given path
    endOfFile = 1;
    SortInfo *sortInfo;
    sortInfo = (SortInfo *) startup;
    runLength = sortInfo->runLength;
    myOrder = sortInfo->myOrder;
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

void* run (void *arg) {
    bigq_util *t = (bigq_util *) arg;
    BigQ b_queue(*(t->in),*(t->out),*(t->sort_order),t->run_len);
}

void SortedDBFile::Add(Record &addme) {
    if (mode == Writing) {
        input->Insert(&addme);
    } else if (mode == Reading) {
        mode = Writing;
        input = new Pipe(BUFFER_SIZE);      // create new instance of the pipes
        output = new Pipe(BUFFER_SIZE);
        util = new bigq_util();
        util->in = input;
        util->out = output;
        util->run_len = runLength;
        util->sort_order = myOrder;
        input->Insert(&addme);
        pthread_create (&bigQThread, NULL, run, (void*)util);
    } else {
        cout << "error" <<endl;
    }
    return;
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

    //get the first records from both
    int fromFile = GetNext(recordFromFile);
    int fromPipe = output->Remove(&recordFromOutputPipe);
    //Schema mySchema ("/Users/vaibhav/Documents/UF CISE/DBI/P1/catalog", "nation");

    cout<<endl;
    //recordFromFile.Print(&mySchema);
    while(true) {
        //get the records from sorted file and compare with recordFromOutputPipe
        if(fromFile && fromPipe) {   // if both have records
            int retVal = ce.Compare(&recordFromFile, &recordFromOutputPipe, myOrder);
            if (retVal == -1 || retVal == 0) { // recordfromFile is lesser
                //add to the tempfile
                //tempFile->Add(recordFromFile);
                if(!page.Append(&recordFromFile)){
                    file.AddPage(&page , pageOffset++);
                    page.EmptyItOut();
                    page.Append(&recordFromFile);
                }
                fromFile = GetNext(recordFromFile);
            } else { // recordFromFile is greater
                //tempFile->Add(recordFromOutputPipe);
                if(!page.Append(&recordFromOutputPipe)){
                    file.AddPage(&page , pageOffset++);
                    page.EmptyItOut();
                    page.Append(&recordFromOutputPipe);
                }
                fromPipe = output->Remove(&recordFromOutputPipe);
            }
        }else if(!fromFile && fromPipe){    // only pipe has records
            //put all the records from pipe to file
            while(fromPipe){
                //tempFile->Add(recordFromOutputPipe);
                if(!page.Append(&recordFromOutputPipe)){
                    file.AddPage(&page , pageOffset++);
                    page.EmptyItOut();
                    page.Append(&recordFromOutputPipe);
                }
                fromPipe = output->Remove(&recordFromOutputPipe);
            }

        }
        else{
            //no records to compare
            break;
        }
    }// while loop ends
    //add the remaining records from file and the pipe
    while(fromFile){
        //file->Add(recordFromFile);
        if(!page.Append(&recordFromFile)){
            file.AddPage(&page , pageOffset++);
            page.EmptyItOut();
            page.Append(&recordFromFile);
        }
        fromFile = GetNext(recordFromFile);
    }

    while(fromPipe){
        //tempFile->Add(recordFromOutputPipe);
        if(!page.Append(&recordFromOutputPipe)){
            file.AddPage(&page , pageOffset++);
            page.EmptyItOut();
            page.Append(&recordFromOutputPipe);
        }
        fromPipe = output->Remove(&recordFromOutputPipe);
    }
    //last page
    file.AddPage(&page,pageOffset++);
    //write to the GenericDBFile file instance
    /*Record temp;
    while (tempFile->GetNext (temp) == 1) {
        if (page.Append(&temp) == 0) {
            file.AddPage(&page,pageOffset++);
            page.EmptyItOut();
            page.Append(&temp);
        }
    }
    tempFile->Close() */
    file.Close();
    return;
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
        mode = Reading;
        MergeFileDataWithQueueData();
    }else{
        pageOffset = 0;         //resetting the page offset to start
        endOfFile = 1;
        return file.Close();
    }
}

int SortedDBFile::GetNext(Record &fetchme) {
    if(mode==Writing) {
        mode = Reading;
        MergeFileDataWithQueueData();
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
        mode = Reading;
        MergeFileDataWithQueueData();
    }else {
        examineCNF(cnf);
    }
}

void SortedDBFile::examineCNF(CNF &cnf) {
    //OrderMaker orderMakerFromCNF;
    Attribute* atts =  orderMakerSchema->GetAtts();

}

int SortedDBFile::Open(const char *fpath) {
    //readMetaData(fpath);
    isBeingRead = true;
    file.Open(1, fpath);   //passing 1 as the first argument to open an already existing file
    isBeingRead = false;
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


