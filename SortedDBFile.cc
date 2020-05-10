//
// Created by Vaibhav Sahay on 25/02/20.
//
#include "SortedDBFile.h"
#include "HeapDBFile.h"
#include "DBFile.h"
#include <iostream>

#define BUFFER_SIZE 100

OrderMaker *myOrder;
int runLength;
Schema *orderMakerSchema;
HeapDBFile tempFile;
char *a = "tmp.tmp";
const char *mainFile;
SortInfo *sortInfo;

SortedDBFile::SortedDBFile() {
    tempFile.Create(a, heap, NULL); //temp file for storing the sorted records
    mode = Reading;
}

SortedDBFile::~SortedDBFile() {
}

//creates an empty file
int SortedDBFile::Create(const char *f_path, fType file_type, void *startup) {
    file.Open(0, f_path);   // passing 0 as the start argument to create a new file at the given path
    endOfFile = 1;
    mainFile = f_path;
    sortInfo = (SortInfo *) startup;
    runLength = sortInfo->runLength;
    myOrder = sortInfo->myOrder;
    return 1;
}

//loads all the records from the given .bin file path
int SortedDBFile::Load(Schema &myschema, char *loadpath) {
    orderMakerSchema = &myschema;
    Record *record;
    FILE *fd = fopen(loadpath, "r");
    if (fd == NULL)    //error while opening thet file
        return -1;
    while (record->SuckNextRecord(&myschema, fd)) { //get records one by one and call Add to add into the input queue
        Add(*record);
    }
    fclose(fd); //closing the file stream
    return 0;
}

//passed to the bigQ thread
void *run(void *arg) {
    bigq_util *t = (bigq_util *) arg;
    BigQ b_queue(*(t->inPipe), *(t->outPipe), *(t->order), t->runlen);
}

//adds the passed record onto the input pipe if in writing mode, else calls bigQ
void SortedDBFile::Add(Record &addme) {
    if (mode == Writing) {
        input->Insert(&addme);
    } else if (mode == Reading) {
        input = new Pipe(BUFFER_SIZE);      // create new instance of the pipes
        output = new Pipe(BUFFER_SIZE);
        util = new bigq_util();
        util->inPipe = input;
        util->outPipe = output;
        util->runlen = runLength;
        util->order = myOrder;
        input->Insert(&addme);
        pthread_create(&bigQThread, NULL, run, (void *) util);   //call BigQ
        mode = Writing;
    } else {
        cout << "error" << endl;
    }
    return;
}

//2-way merge of records from the file and output queue
void SortedDBFile::MergeFileDataWithQueueData() {
    //shutdown input pipe
    input->ShutDown();
    mode = Reading;
    //remove the records from output pipe one by one and check with the sorted file to merge
    ComparisonEngine ce;
    Record recordFromOutputPipe;
    Record recordFromFile;
    int i = 0;
    if (file.GetLength() != 0) {
        MoveFirst();
    }
    //get the start records from both
    int fromFile = GetNext(recordFromFile);
    int fromPipe = output->Remove(&recordFromOutputPipe);
    while (true) {
        //get the records from sorted file and compare with recordFromOutputPipe
        if (fromFile && fromPipe) {   // if both have records
            int retVal = ce.Compare(&recordFromFile, &recordFromOutputPipe, myOrder);
            if (retVal < 1) { // recordfromFile is lesser
                //add to the tempfile
                tempFile.Add(recordFromFile);
                fromFile = GetNext(recordFromFile);
            } else { // recordFromFile is greater
                tempFile.Add(recordFromOutputPipe);
                fromPipe = output->Remove(&recordFromOutputPipe);
            }
        } else {
            //only one of the file or pipe has records so just add all of it in the next loop
            break;
        }
    }// while loop ends
    //add the remaining records from file and the pipe
    while (fromFile) {
        tempFile.Add(recordFromFile);
        fromFile = GetNext(recordFromFile);
    }

    while (fromPipe) {
        tempFile.Add(recordFromOutputPipe);
        fromPipe = output->Remove(&recordFromOutputPipe);
    }

    //write the records to disk
    tempFile.Close1();
    //rename the temp file as the main file
    rename(a, mainFile);
    file.Open(1, mainFile);
    pageOffset = 0;
}

//clears the queues
void SortedDBFile::clearQueues() {
    delete util;
    delete input;
    delete output;
    input = NULL;
    output = NULL;
}

//moves the page offset to the start of the file
void SortedDBFile::MoveFirst() {
    if (mode == Writing) {
        mode = Reading;
        input->ShutDown();
        MergeFileDataWithQueueData();
        clearQueues();
    } else {
        pageOffset = 0;
    }
}

// closed the sorted file
int SortedDBFile::Close() {
    if (mode == Writing) {
        mode = Reading;
        MergeFileDataWithQueueData();
        clearQueues();
    } else {
        pageOffset = 0;         //resetting the page offset to start
        endOfFile = 1;
        return file.Close();
    }
}

int SortedDBFile::GetNext(Record &fetchme) {
    if (mode == Writing) {
        mode = Reading;
        MergeFileDataWithQueueData();
        clearQueues();
    } else {
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

void SortedDBFile::examineCNF(CNF &cnf) {
    //OrderMaker orderMakerFromCNF;
    Attribute *atts = orderMakerSchema->GetAtts();
}

//reads from the corresponding metadata file and opens the file
int SortedDBFile::Open(const char *fpath) {
    readMetaData(fpath);
    isBeingRead = true;
    file.Open(1, fpath);   //passing 1 as the start argument to open an already existing file
    isBeingRead = false;
    return 0;
}

//reads the metadata file for the fpath
int SortedDBFile::readMetaData(const char *fpath) {
    string metafileName;
    string type;
    string runLength;
    metafileName.append(fpath);
    metafileName.append(".meta");
    ifstream metafile;
    metafile.open(metafileName.c_str());
    if (!metafile)
        return -1;
    int count = 0;
    string line;

    while (getline(metafile, line)) {
        if (count == 0) {
            type = line;    // file type
            count++;
        } else if (count == 1) {
            runLength = line;   //run length
            count++;
        }
    }
    cout << "File type=" << type << endl;
    cout << "Run Length=" << runLength << endl;
    metafile.close();
    return 0;
}

//finds the page of the record from the CNF
int SortedDBFile::FindFromCNF(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine ce;
    while (true) {
        if (!(page.GetFirst(&fetchme))) {
            pageOffset++;
            if (pageOffset < file.GetLength() - 1)
                file.GetPage(&page, pageOffset);
            else
                return 0;
        } else {
            if (ce.Compare(&fetchme, &literal, &cnf))
                return 1;
        }
    }
}

//finds the page of the record from the myorder
int SortedDBFile::FindFromSortOrder(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine ce;
    while (true) {
        if (!(page.GetFirst(&fetchme))) {
            pageOffset++;
            if (pageOffset < file.GetLength() - 1)
                file.GetPage(&page, pageOffset);
            else
                return 0;
        } else {
            if (ce.Compare(&literal, query, &fetchme, myOrder) == 0) {
                if (ce.Compare(&fetchme, &literal, &cnf))
                    return 1;
            } else
                return 0;
        }
    }
}

//Gets the filtered records after calling binary search on the file
int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    /*if (mode == Writing) {
        MergeFileDataWithQueueData();
        queryBuilt = false;
        sortOrderExists = true;
    }
    if (sortOrderExists) {
        if (!queryBuilt) {
            queryBuilt = true;
            int result;
            query = new OrderMaker;
            if (cnf.GetQuerySortOrders(*query, *myOrder) > 0) {
                result = BinarySearch(fetchme, cnf, literal);
                ComparisonEngine ce;
                if (result != 0) {
                    if (ce.Compare(&fetchme, &literal, &cnf))
                        return 1;
                    else {
                        FindFromSortOrder(fetchme, cnf, literal);
                    }
                } else
                    return 0;
            } else {
                sortOrderExists = false;
                return FindFromCNF(fetchme, cnf, literal);
            }
        } else
            FindFromSortOrder(fetchme, cnf, literal);
    } else
        return FindFromCNF(fetchme, cnf, literal); */
    ComparisonEngine ce;
    int ret;
    do {
        if (GetNext(fetchme) == 1)
            ret = ce.Compare (&fetchme, &literal, &cnf);
        else return 0;
    } while(ret == 0);
    return ret;
}

int SortedDBFile::BinarySearch(Record &fetchme, CNF &cnf, Record &literal) {
    off_t start = pageOffset;
    off_t end = file.GetLength() - 2;
    Record *currentRecord = new Record;
    Page *midPage = new Page;
    bool found = false;
    ComparisonEngine engine;
    off_t mid = start;
    while (start < end) {
        mid = (start + end) / 2;
        file.GetPage(midPage, mid);
        if (midPage->GetFirst(&fetchme) == 1) {
            if (engine.Compare(&literal, query, &fetchme, myOrder) <= 0)
                end = mid;
            else {
                start = mid;
                if (start == end - 1) {
                    file.GetPage(midPage, end);
                    midPage->GetFirst(&fetchme);
                    if (engine.Compare(&literal, query, &fetchme, myOrder) > 0)
                        mid = end;
                    break;
                }
            }
        } else
            break;
    }
    if (mid == pageOffset) {
        while (page.GetFirst(&fetchme) == 1) {
            if (engine.Compare(&literal, query, &fetchme, myOrder) == 0) {
                found = true;
                break;
            }
        }
    } else {
        file.GetPage(&page, mid);
        while (page.GetFirst(&fetchme) == 1) {
            if (engine.Compare(&literal, query, &fetchme, myOrder) == 0) {
                found = true;
                pageOffset = mid;
                break;
            }
        }
    }
    if (!found && mid < file.GetLength() - 2) {
        file.GetPage(&page, mid + 1);
        if (page.GetFirst(&fetchme) == 1 && engine.Compare(&literal, query, &fetchme, myOrder) == 0) {
            found = true;
            pageOffset = mid + 1;
        }
    }
    return (found ? 1 : 0);
}