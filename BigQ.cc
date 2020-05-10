#include "BigQ.h"
void *runBigQ(void *arg) {
    File f;
    char temp_file[100];
    sprintf(temp_file, "temp%d.bin", rand());
    f.Open(0, temp_file);  //Tempfile for maintaining sorted run
    bigq_util *bigQ = (bigq_util *) arg;
    Page currentPage;
    off_t pageIndex = 0;
    Record temp, *rec;
    Record *newrec;
    int counter = 0;
    int popped = 0;
    int pushed = 0;
    static int bufferCount;
    PQ pq(ComparePQ(bigQ->order));
    CompareTwoRecords recCompare(bigQ->order);
    currentPage.EmptyItOut();
    vector<Record *> recArr;
    //get the size of each run
    int runSize = bigQ->runlen * PAGE_SIZE;
    int curSizeInBytes = sizeof(int);
    int recSize = 0;
    vector<int> pageIndexArr;
    int runCount = 0;
    Pipe *in = bigQ->inPipe;
    //while input pipe has records
    while (in->Remove(&temp)) {
        //recSize = (&temp)->GetSize();
        newrec = new Record;
        newrec->Consume(&temp);

        if (runCount <= bigQ->runlen) {
            //push to record vector
            recArr.push_back(newrec);
            pushed++;
            curSizeInBytes += recSize;
        } else {
            // completed one run with runlen pages
            runCount++;
            pageIndexArr.push_back(pageIndex);
            //sort record array
            sort(recArr.begin(), recArr.end(), recCompare);
            //write to file
            //writeRunsToFile();

            for (int i = 0; i < recArr.size(); i++) {
                rec = recArr.at(i);
                if (currentPage.Append(rec) == 0) {
                    //page full, so add the page to file, empty it and write the record
                    f.AddPage(&currentPage, pageIndex++);
                    currentPage.EmptyItOut();
                    currentPage.Append(rec);
                }
                delete rec;
            }
            recArr.clear();
            //write last page if it has records
            if (currentPage.getCurSizeInBytes() > 0) {
                f.AddPage(&currentPage, pageIndex++);
                currentPage.EmptyItOut();
            }
            recArr.push_back(newrec);

            curSizeInBytes = sizeof(int) + recSize;
        }
    }
    //sort the records of last page
    vector<Record*>::iterator iter1 = recArr.begin();
    vector<Record*>::iterator iter2 = recArr.end();

    sort(recArr.begin(), recArr.end(), recCompare);
    //write to the vector
    pageIndexArr.push_back(pageIndex);
    for (int i = 0; i < recArr.size(); i++) {
        rec = recArr.at(i);
        if (currentPage.Append(rec) == 0) {
            f.AddPage(&currentPage, pageIndex++);
            currentPage.EmptyItOut();
            currentPage.Append(rec);
        }
        delete rec;
    }
    recArr.clear();
    if (currentPage.getCurSizeInBytes() > 0) {
        f.AddPage(&currentPage, pageIndex++);
        currentPage.EmptyItOut();
    }

    //merge phase
    pageIndexArr.push_back(pageIndex);
    int numOfRuns = pageIndexArr.size() - 1;
    Run *runs[numOfRuns];
    for (int i = 0; i < numOfRuns; i++) {
        Record *tmprec = new Record;
        runs[i] = new Run(&f, pageIndexArr[i], pageIndexArr[i + 1]);
        runs[i]->GetNext(tmprec);
        RunRecord *runRecord = new RunRecord(tmprec, runs[i]);
        pq.push(runRecord);
    }
    RunRecord *popPQ;
    Record *popRec;
    Run *popRun;
    while (!pq.empty()) {
        popPQ = pq.top();
        pq.pop();
        popRun = popPQ->run;
        popRec = popPQ->record;
        bigQ->outPipe->Insert(popRec);
        delete popRec;
        Record *nextRecord = new Record;
        if (popRun->GetNext(nextRecord) == 1) {
            popPQ->record = nextRecord;
            pq.push(popPQ);
        } else {
            delete popRun;
            delete popPQ;
            delete nextRecord;
        }
    }
    bigQ->outPipe->ShutDown();
    f.Close();
    remove(temp_file);
}


BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t myThread;
    //create and init the struct
    bigq_util *bq = new bigq_util;
    bq->order = &sortorder;
    bq->runlen = runlen;
    bq->outPipe = &out;
    bq->inPipe = &in;

    //create and start the thread
    pthread_create(&myThread, NULL, runBigQ, bq);
    //pthread_join(myThread,NULL);
    //sout.ShutDown();
}

BigQ::~BigQ() {
}

ComparePQ::ComparePQ(OrderMaker *order) {
    this->order = order;
}

bool ComparePQ::operator()(RunRecord *left, RunRecord *right) const {
    ComparisonEngine ce;
    if (ce.Compare(left->record, right->record, order) >= 0)
        return true;
    else
        return false;
}

CompareTwoRecords::CompareTwoRecords(OrderMaker *order) {
    this->order = order;
}

bool CompareTwoRecords::operator()(Record *left, Record *right) const {
    ComparisonEngine ce;
    if (ce.Compare(left, right, order) < 0)
        return true;
    else
        return false;
}
