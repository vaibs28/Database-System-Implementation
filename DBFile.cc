#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedDBFile.h"
#include "HeapDBFile.h"
#include <iostream>


//constructor initializing the required objects
DBFile::DBFile() {
    genericDbFile = NULL;
}

//destructor
DBFile::~DBFile() {
}

//loads the DBFile instance from a textfile
int DBFile::Load(Schema &f_schema, char *loadpath) {
    if(genericDbFile!=NULL)
        return genericDbFile->Load(f_schema, loadpath);
    return -1;
}

void DBFile::Add(Record &addMe) {
    if(genericDbFile!=NULL)
        genericDbFile->Add(addMe);
}

void DBFile::MoveFirst() {
    //move to the first page
    genericDbFile->MoveFirst();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type == heap) {
        genericDbFile = new HeapDBFile();
    } else if (f_type == sorted) {
        genericDbFile = new SortedDBFile();
    }else{
        cerr << "Not a valid type"<<endl;
        return -1;
    }
    genericDbFile->Create(f_path, f_type, startup);
    //create metadata file
    fstream metaFile;
    string metaFileName;
    metaFileName.append(f_path);
    metaFileName.append(".meta");
    metaFile.open(metaFileName,ios::out);
    if(!metaFile)
    {
        cout<<"Error in creating Metafile!!!";
        return 0;
    }
    cout<<"Metafile created successfully"<<endl;
    //write to metafile
    string fileTypeToBeWritten;
    SortInfo *sortInfo;
    OrderMaker* myOrder;
    int runLength;
    if(f_type==0)
        fileTypeToBeWritten = "heap";
    else if(f_type==1) {
        fileTypeToBeWritten = "sorted";
        sortInfo = (SortInfo *) startup;
        runLength = sortInfo->runLength;
        myOrder = sortInfo->myOrder;
    }
    metaFile << fileTypeToBeWritten<<endl;
    if(f_type==1) {
        metaFile << runLength << endl;
        myOrder->WriteToMetaFile(metaFile);
    }
    return 1;
}


int DBFile::Open(const char *f_path) {
    //read metadata and invoke the corresponding file instance
    string type;
    string metafileName;
    string line;
    metafileName.append(f_path);
    metafileName.append(".meta");
    ifstream metafile(metafileName);
    int count = 0;
    if(metafile.is_open()){
        while (getline(metafile , line)){
            if(count==0){
                type = line;
                count++;
            }
        }
    }
    fType dbfileType;
    if(type=="heap"){
        dbfileType = heap;
    }else if(type=="sorted"){
        dbfileType = sorted;
    }

    metafile.close();
    if(dbfileType == heap && genericDbFile==NULL){
        //instantiate heapdbfile
        genericDbFile = new HeapDBFile();
    }else if(dbfileType == sorted && genericDbFile==NULL){
        genericDbFile = new SortedDBFile();
    }else{
        cout<<"no implementation available";
    }
    //open the file
    return genericDbFile->Open(f_path);
}


int DBFile::Close() {
    if(genericDbFile!=NULL)
        return genericDbFile->Close();
    return -1;
}


int DBFile::GetNext(Record &fetchme) {
    if(genericDbFile!=NULL)
        return genericDbFile->GetNext(fetchme);
    return -1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if(genericDbFile!=NULL)
        return genericDbFile->GetNext(fetchme, cnf, literal);
    return -1;
}