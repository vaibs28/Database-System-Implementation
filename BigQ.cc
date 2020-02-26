#include "BigQ.h"
#include <cstdlib>
#include "CustomComparator.h"
#include "CustomComparator2.h"

using namespace std;

vector<Record *> recordArray; //list of  pointer to a Record
vector<Page *> pageArray;     //list of pointer to a Page
map<int, Page *> pageMap;  
int pagesWrittenToFile = 0;  //global index for pages in file
char temp_file[100];

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    //will be called from the main thread itself, not creating another thread
    int pageCount = 0;                          // using to keep track of the number of pages, max could be runlen
    int retVal = createTempFile();
    currentPage = new Page();
    while (true) {  // keep reading from the input buffer until it is empty
        if (in.Remove(&rec) &&
            pageCount < runlen) {    //if buffer has records and pageCount less than runlen, append records to Page
            if (!currentPage->Append(&rec)) {
                pageArray.push_back(currentPage); // push to pageArray since the current page is full.
                generateRuns(sortorder);
                //write the runs to temp file
                writeRunsToFile(runlen);
                currentPage = new Page();         // create new Page instance
                currentPage->Append(&rec);        // append record
                pageCount++;                      // increment pageCount by 1
            }
        } else {
            if (pageCount < runlen && currentPage->getRecordCount()) {
                pageArray.push_back(currentPage);    // if the last page has records, add it to the pageArray
            }
            //generate sorted runs
            generateRuns(sortorder);
            //write the runs to temp file
            writeRunsToFile(runlen);
            if (pageCount >= runlen) {
                currentPage->Append(&rec);
                pageCount = 0;
                continue;
            } else {
                break;
            }
        }
    } // while loop ends
    //merge phase
    off_t fileLength = f.GetLength();
    off_t numRuns = 0;
    if (fileLength != 0)
        numRuns = ((ceil)((float) (fileLength - 1) / runlen));
    else numRuns = 0;
    off_t lastPage = (fileLength - 1) - (numRuns - 1) * runlen;
    priority_queue<Record *, vector<Record *>, CustomComparator2> pq(&sortorder);
    //Maps Record to run
    map<Record *, int> m_record;
    int *pageOffset = new int[numRuns];
    Page **pageArr = new Page *[numRuns];
    int page_num = 0;
    for (int i = 0; i < numRuns; i++) {
        pageArr[i] = new Page();
        f.GetPage(pageArr[i], page_num);
        pageOffset[i] = 1;
        Record *r = new Record;
        pageArr[i]->GetFirst(r);
        pq.push(r);
        m_record[r] = i;
        r = NULL;
        page_num += runlen;
    }
    while (!pq.empty()) {
        Record *r = pq.top(); //get the minimum record
        pq.pop();
        int nextRecord = -1;
        nextRecord = m_record[r];
        m_record.erase(r);
        if (nextRecord == -1)
            return;
        Record *next = new Record;
        bool is_rec_found = true;
        if (!pageArr[nextRecord]->GetFirst(next)) {
            //Check if not the end of run
            if ((!(nextRecord == (numRuns - 1)) && pageOffset[nextRecord] < runlen)
                || ((nextRecord == (numRuns - 1) && pageOffset[nextRecord] < lastPage))) {
                f.GetPage(pageArr[nextRecord], pageOffset[nextRecord] + nextRecord * runlen);
                pageArr[nextRecord]->GetFirst(next);
                pageOffset[nextRecord]++;
            } else {  //last run
                if (pageOffset[nextRecord] == runlen) {
                    if (pageMap[(nextRecord + 1) * runlen - 1]) {
                        delete pageArr[nextRecord];
                        pageArr[nextRecord] = NULL;
                        pageArr[nextRecord] = (pageMap[(nextRecord + 1) * runlen - 1]);
                        pageMap[(nextRecord + 1) * runlen - 1] = NULL;
                        pageArr[nextRecord]->GetFirst(next);
                    } else is_rec_found = false;
                } else is_rec_found = false;
            }
        }
        //Push the next record into the priority queue if found
        if (is_rec_found) {
            pq.push(next);
        }
        m_record[next] = nextRecord;
        out.Insert(r);
    }
    f.Close();
    out.ShutDown();
}

BigQ::~BigQ() {}

int BigQ::generateRuns(OrderMaker &sortorder) {
    for (int i = 0; i < pageArray.size(); i++) {
        Record *temp = new Record();
        while (pageArray[i]->GetFirst(temp)) {
            recordArray.push_back(temp);
            temp = new Record();
        }
    }
    //internal sort all the records using stdlib
    try {
        std::sort(recordArray.begin(), recordArray.end(), CustomComparator(&sortorder));
    }
    catch (exception e){
        return -1;
    }

    return 0;
}

void BigQ::writeRunsToFile(int runlen) {
    int pageIndex = 0;
    Page *runPage = new Page();
    int pageCount1 = 0;
    for (int i = 0;
         i < recordArray.size(); i++) {  //traverse through all the records and put them in pages and then into file
        if (!runPage->Append(recordArray[i])) {     //page is full
            pageIndex++;                            //increment the index
            f.AddPage(runPage, pagesWrittenToFile++); //add to file
            runPage->EmptyItOut();                  //clear the page
            runPage->Append(recordArray[i]);        //add the next record as first in the empty page
        }
    }

    //last page
    if (pageIndex < runlen) {
        if (runPage->getRecordCount())
            f.AddPage(runPage, pagesWrittenToFile++);
    } else {
        if (runPage->getRecordCount()) {
            pageMap[pagesWrittenToFile - 1] = runPage;
        }
    }
    recordArray.clear();
    pageArray.clear();
}

int BigQ::createTempFile() {
    sprintf(temp_file, "temp%d.bin", rand());
    try {
        f.Open(0, temp_file);                //Tempfile for maintaining sorted run
    }
    catch(exception e){
        return -1;
    }
    return 0;
}
