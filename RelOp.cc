#include "RelOp.h"

using namespace std;

/******  Select Pipe Implementation   ********/

void *SetupThreadSelectPipe(void *arg) {
    ((SelectPipe *) arg)->StartOperation();
}

void SelectPipe::StartOperation() {
    Record record;
    ComparisonEngine ce;
    //write every record to from input pipe to output pipe if cnf is satisfied
    while (input->Remove(&record)) {
        if (ce.Compare(&record, rec, cnf)) {
            output->Insert(&record);
        }
    }
    //shutdown the output pipe
    output->ShutDown();
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->input = &inPipe;
    this->output = &outPipe;
    this->cnf = &selOp;
    this->rec = &literal;
    pthread_create(&myThread, NULL, SetupThreadSelectPipe, (void *) this);
}

void SelectPipe::WaitUntilDone() {
    //waits for the thread to complete execution
    pthread_join(myThread, NULL);
}

void SelectPipe::Use_n_Pages(int n) {
    this->runlen = n;
}

/******* Select File Implementation**********/
void *SetupThreadSelectFile(void *arg) {
    ((SelectFile *) arg)->StartOperation();
}

void SelectFile::StartOperation() {
    Record record;
    //start from the first record
    dbfile->MoveFirst();
    //while record is present, apply cnf and add to output pipe
    if(cnf==NULL){
        cnf = new CNF();
    }
    while (dbfile->GetNext(record, *cnf, *rec)) {
        output->Insert(&record);
    }
    output->ShutDown();
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->dbfile = &inFile;
    this->output = &outPipe;
    this->cnf = &selOp;
    this->rec = &literal;
    pthread_create(&myThread, NULL, SetupThreadSelectFile, (void *) this);
}

void SelectFile::WaitUntilDone() {
    //waits for the thread to complete execution
    //pthread_join(myThread, NULL);
}

void SelectFile::Use_n_Pages(int n) {
    this->runlen = n;
}


/********   Project Implementation   *****************/

void *SetupThreadProject(void *arg) {
    ((Project *) arg)->StartOperation();
}

void Project::StartOperation() {
    Record record;
    //writes records to the output pipe with keepAtts
    while (input->Remove(&record)) {
        if(input->lastSlot == input->firstSlot)
            input->done = 1;
        record.Project(keepAtts, noAttsOut, noAttsIn);
        output->Insert(&record);
    }
    output->ShutDown();
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->input = &inPipe;
    this->output = &outPipe;
    this->keepAtts = keepMe;
    this->noAttsIn = numAttsInput;
    this->noAttsOut = numAttsOutput;
    pthread_create(&myThread, NULL, SetupThreadProject, (void *) this);
}

void Project::WaitUntilDone() {
    //waits for the thread to complete execution
    pthread_join(myThread, NULL);
}

void Project::Use_n_Pages(int n) {
    this->runlen = n;
}

/***********   Join Implementation   *******************/

void *SetupThreadJoin(void *arg) {
    ((Join *) arg)->StartOperation();
}

void Join::StartOperation() {

    int leftAttributes;
    int rightAttributes;

    vector < Record * > leftRecVector;
    vector < Record * > rightRecVactor;

    Record *LeftRec = new Record();
    Record *RightRec = new Record();
    Record *JoinRec = new Record();

    OrderMaker leftOrder, rightOrder;
    ComparisonEngine comp;

    Pipe *leftPipe = new Pipe(100);
    Pipe *rightPipe = new Pipe(100);

    bool leftFlag, rightFlag = false;


    if (myCNF->GetSortOrders(leftOrder, rightOrder)) {
        //left bigQ thread instantiated, all left records will be in left pipe
        BigQ bigqleft(*inLeft, *leftPipe, leftOrder, runlen); 

        //right BigQ thread instantiated, all right records will be in right pipe
        BigQ bigqright(*inRight, *rightPipe, rightOrder, runlen); 
        leftPipe->Remove(LeftRec);
        rightPipe->Remove(RightRec);

        leftAttributes = ((int *) LeftRec->bits)[1] / sizeof(int) - 1;
        rightAttributes = ((int *) RightRec->bits)[1] / sizeof(int) - 1;

        //to store the joined attributes count
        int attributesArray[leftAttributes + rightAttributes];
        for (int i = 0; i < leftAttributes; i++)
            attributesArray[i] = i;                     // count of left table attributes
        for (int i = 0; i < rightAttributes; i++)
            attributesArray[leftAttributes + i] = i;    //count of right table attributes

        while (true) {
            if (comp.Compare(LeftRec, &leftOrder, RightRec, &rightOrder) < 0) {
                if (leftPipe->Remove(LeftRec) != 1)
                    break;
            } else if (comp.Compare(LeftRec, &leftOrder, RightRec, &rightOrder) > 0) {
                if (rightPipe->Remove(RightRec) != 1)
                    break;
            } else {
                // for equal records
                leftFlag = false;
                rightFlag = false;

                //populate left vector
                while (true) {
                    Record *temporary = new Record;
                    temporary->Consume(LeftRec);
                    leftRecVector.push_back(temporary);
                    if (leftPipe->Remove(LeftRec) != 1) {
                        leftFlag = true;
                        break;
                    }
                    if (comp.Compare(temporary, LeftRec, &leftOrder) != 0)
                        break;
                }
                //populate right vector
                while (true) {
                    Record *temporary = new Record;
                    temporary->Consume(RightRec);
                    rightRecVactor.push_back(temporary);
                    if (rightPipe->Remove(RightRec) != 1) {
                        rightFlag = true;
                        break;
                    }
                    if (comp.Compare(temporary, RightRec, &rightOrder) != 0)
                        break;
                }

                //join by calling mergeRecords
                for (int i = 0; i < leftRecVector.size(); i++) {
                    for (int j = 0; j < rightRecVactor.size(); j++) {
                        JoinRec->MergeRecords(leftRecVector.at(i), rightRecVactor.at(j), leftAttributes,
                                              rightAttributes, attributesArray,
                                              leftAttributes + rightAttributes, leftAttributes);
                        output->Insert(JoinRec);
                    }
                }
                leftRecVector.clear();
                rightRecVactor.clear();

                if (leftFlag || rightFlag)
                    break;
            }
        }
    } else {
        //defaults to nested-block join
        while (inRight->Remove(RightRec) == 1) {
            Record temp;
            temp.Consume(RightRec);
            rightRecVactor.push_back(&temp);
        }
        inLeft->Remove(LeftRec);
        leftAttributes = ((int *) LeftRec->bits)[1] / sizeof(int) - 1;
        rightAttributes = ((int *) rightRecVactor.at(0)->bits)[1] / sizeof(int) - 1;

        int attributesArray[leftAttributes + rightAttributes];
        for (int i = 0; i < leftAttributes; i++)
            attributesArray[i] = i;
        for (int i = 0; i < rightAttributes; i++)
            attributesArray[leftAttributes + i] = i;
        while (inLeft->Remove(LeftRec) == 1) {
            for (int j = 0; j < rightRecVactor.size(); j++) {
                JoinRec->MergeRecords(LeftRec, rightRecVactor.at(j), leftAttributes, rightAttributes, attributesArray,
                                      leftAttributes + rightAttributes,
                                      leftAttributes);
                output->Insert(JoinRec);
            }
        }
    }
    output->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inLeft = &inPipeL;
    this->inRight = &inPipeR;
    this->output = &outPipe;
    this->myCNF = &selOp;
    this->rec = &literal;
    pthread_create(&myThread, NULL, SetupThreadJoin, (void *) this);
}

void Join::WaitUntilDone() {
    //waits for the thread to complete execution
    pthread_join(myThread, NULL);
}

void Join::Use_n_Pages(int n) {
    this->runlen = n;
}

/*************   Duplicate Removal Implementation   *************************/

void *SetupThreadDuplicateRemoval(void *arg) {
    ((DuplicateRemoval *) arg)->StartOperation();
}

void DuplicateRemoval::StartOperation() {
    Record temp, next;
    OrderMaker *sortorder = new OrderMaker(schema);
    Pipe *pipe = new Pipe(100);
    BigQ bigq(*input, *pipe, *sortorder, 10);
    ComparisonEngine comp;
    pipe->Remove(&next);
    temp.Copy(&next);
    output->Insert(&next);
    // write to output pipe if records are different
    while (pipe->Remove(&next) == 1) {
        if (comp.Compare(&temp, &next, sortorder) != 0) {
            temp.Copy(&next);
            output->Insert(&next);
        }
    }
    input->ShutDown();
    output->ShutDown();
    delete pipe;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    this->input = &inPipe;
    this->output = &outPipe;
    this->schema = &mySchema;
    pthread_create(&myThread, NULL, SetupThreadDuplicateRemoval, (void *) this);
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(myThread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n) {
    this->runlen = n;
}

/**************   Sum Implementation   *************************/

void *SetupThreadSum(void *arg) {
    ((Sum *) arg)->StartOperation();
}

void Sum::StartOperation() {
    Record temp, groupSum;
    Type mytype;
    int sumIntRecords, totalSumInt = 0;
    double sumDoubleRecords, totalSumDouble = 0;
    if(!distinct) {
        while (input->Remove(&groupSum)) {
            compute->Apply(groupSum, sumIntRecords, sumDoubleRecords);
            if (compute->returnsInt == 1) {
                mytype = Int;
                totalSumInt = totalSumInt + sumIntRecords;
            } else {
                mytype = Double;
                totalSumDouble = totalSumDouble + sumDoubleRecords;
            }
        }
        double finalSum = totalSumInt + totalSumDouble;
        temp.ComposeRecord(&sumSchema, (std::to_string(finalSum) + "|").c_str());
        //temp.CreateNewRecord(mytype, totalSumInt, totalSumDouble);
        output->Insert(&temp);
        output->ShutDown();
    }else{
        int intVal = 0;
        double doubleVal = 0;
        DuplicateRemoval dr;
        Pipe drInPipe(100), drOutPipe(100);
        dr.Run(drInPipe, drOutPipe, sumSchema);
        Record *temp = new Record();
        while (input->Remove(temp)) {
            intVal = 0;
            doubleVal = 0;
            compute->Apply(*temp, intVal, doubleVal);

            temp->ComposeRecord(&sumSchema, (std::to_string(intVal + doubleVal) + "|").c_str());

            drInPipe.Insert(temp);
            temp = new Record();
        }
        drInPipe.ShutDown();

        double sum = 0;
        while (drOutPipe.Remove(temp)) {

            int pointer = ((int *) temp->bits)[1];
            double *myDouble = (double *) &(temp->bits[pointer]);

            sum += *myDouble;
        }

        dr.WaitUntilDone();

        temp->ComposeRecord(&sumSchema, (std::to_string(sum) + "|").c_str());
        output->Insert(temp);
    }
    output->ShutDown();
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe,int distinct) {
    this->input = &inPipe;
    this->output = &outPipe;
    this->compute = &computeMe;
    this->distinct = distinct;
    pthread_create(&myThread, NULL, SetupThreadSum, (void *) this);
}

void Sum::WaitUntilDone() {
    //waits for the thread to complete execution
    pthread_join(myThread, NULL);
}

void Sum::Use_n_Pages(int n) {
    this->runlen = n;
}


/*****************   GroupBy Implementation  **************************/

void *SetupThreadGroupBy(void *arg) {
    ((GroupBy *) arg)->StartOperation();
}

void AddGroupByRecordToPipe(Pipe *outputPipe, Record *tableRecord, Record *sumRecord, OrderMaker *order) {
    Schema groupAttributesSchema(*order);
    tableRecord->Project(order->GetAtts(), order->GetNumAtts());
    Record groupRecord;
    groupRecord.MergeRecords(sumRecord, tableRecord);
    outputPipe->Insert(&groupRecord);

}

void GroupBy::StartOperation() {
    /*Record records[2];
    Record *startRec = NULL, *endRec = NULL;
    Record *sumOfTempRecs = new Record;
    Record *newRecord = new Record;
    Type myType;
    Pipe *pipe = new Pipe(100);
    BigQ bigq(*input, *pipe, *myorder, 10);
    ComparisonEngine comp;
    int totAtts = (myorder->numAtts) + 1;
    int groupId = 0, totalSumInt = 0;
    int recordTotalSumInt;
    double totalSumDouble = 0, sumDoubleRecs;
    int attributesArray[totAtts];


    attributesArray[0] = 0;
    for (int i = 1; i < totAtts; i++)
        attributesArray[i] = myorder->whichAtts[i - 1];

    while (pipe->Remove(&records[groupId % 2]) == 1) {
        startRec = endRec;
        endRec = &records[groupId % 2];
        if (startRec != NULL && endRec != NULL) {
            if (comp.Compare(startRec, endRec, myorder) != 0) {
                compute->Apply(*startRec, recordTotalSumInt, sumDoubleRecs);
                if (compute->returnsInt == 1) {
                    myType = Int;
                    totalSumInt = totalSumInt + recordTotalSumInt;
                } else {
                    myType = Double;
                    totalSumDouble = totalSumDouble + sumDoubleRecs;
                }
                int start = ((int *) startRec->bits)[1] / sizeof(int) - 1;

                newRecord->CreateNewRecord(myType, totalSumInt, totalSumDouble);
                sumOfTempRecs->MergeRecords(newRecord, startRec, 1, start, attributesArray, totAtts, 1);
                output->Insert(sumOfTempRecs);
                totalSumInt = 0;
                totalSumDouble = 0;
            } else {
                compute->Apply(*startRec, recordTotalSumInt, sumDoubleRecs);
                if (compute->returnsInt == 1) {
                    myType = Int;
                    totalSumInt = totalSumInt + recordTotalSumInt;
                } else {
                    myType = Double;
                    totalSumDouble = totalSumDouble + sumDoubleRecs;
                }
            }
        }
        groupId++;
    }

    compute->Apply(*endRec, recordTotalSumInt, sumDoubleRecs);
    if (compute->returnsInt == 1) {
        myType = Int;
        totalSumInt = totalSumInt + recordTotalSumInt;
    } else {
        myType = Double;
        totalSumDouble = totalSumDouble + sumDoubleRecs;
    }
    int start = 0;
    if(startRec!=NULL)
         start = ((int *) startRec->bits)[1] / sizeof(int) - 1;
    double  finalSum = totalSumInt + totalSumDouble;
    newRecord->ComposeRecord(&sumSchema, (std::to_string(finalSum) + "|").c_str());
    sumOfTempRecs->MergeRecords(newRecord, endRec, 1, start, attributesArray, totAtts, 1);
    output->Insert(newRecord);
    output->ShutDown();*/
     Pipe bigQOutputPipe(100);
    BigQ(*this->input, bigQOutputPipe, *this->myorder, this->runlen);

    Pipe sumInPipe(100), sumOutPipe(100);
    Sum sum;
    sum.Run(sumInPipe, sumOutPipe, *this->compute, this->distinctFunc);

    int i = 0;
    Record recs[2];
    Record *temp = new Record();

    ComparisonEngine comparisonEngine;
    if (bigQOutputPipe.Remove(&recs[i % 2])) {
        temp->Copy(&recs[i % 2]);
        sumInPipe.Insert(temp);
        temp = new Record();
        i++;
        while (bigQOutputPipe.Remove(&recs[i % 2])) {
            if (comparisonEngine.Compare(&recs[(i + 1) % 2], &recs[i % 2], this->myorder) != 0) {
                sumInPipe.ShutDown();
                sumOutPipe.Remove(temp);
                sum.WaitUntilDone();
                AddGroupByRecordToPipe(this->output, &recs[(i + 1) % 2], temp, this->myorder);
                sumInPipe = *(new Pipe(100));
                sumOutPipe = *(new Pipe(100));
                sum.Run(sumInPipe, sumOutPipe, *this->compute, this->distinctFunc);
            }
            temp->Copy(&recs[i % 2]);
            sumInPipe.Insert(temp);
            temp = new Record();
            i++;
        }
        sumInPipe.ShutDown();
        sumOutPipe.Remove(temp);
        sum.WaitUntilDone();
        AddGroupByRecordToPipe(this->output, &recs[(i + 1) % 2], temp, this->myorder);
    }
output->ShutDown();

}


void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, int distinctFunc) {
    this->input = &inPipe;
    this->output = &outPipe;
    this->myorder = &groupAtts;
    this->compute = &computeMe;
    this-> distinctFunc = distinctFunc;
    pthread_create(&myThread, NULL, SetupThreadGroupBy, (void *) this);
}

void GroupBy::WaitUntilDone() {
    //pthread_join(myThread, NULL);
}

void GroupBy::Use_n_Pages(int n) {
    this->runlen = n;
}


/************   WriteOut Implementation   *****************/

void *SetupThreadWriteOut(void *arg) {
    ((WriteOut *) arg)->StartOperation();
}

void WriteOut::StartOperation() {
    Record temp;
    while (input->Remove(&temp))
        temp.WriteRecordsToFile(schema, file);
    fclose(file);
    input->ShutDown();
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    this->input = &inPipe;
    this->file = outFile;
    this->schema = &mySchema;
    pthread_create(&myThread, NULL, SetupThreadWriteOut, (void *) this);
}

void WriteOut::WaitUntilDone() {
    pthread_join(myThread, NULL);
}

void WriteOut::Use_n_Pages(int n) {
    this->runlen = n;
}




