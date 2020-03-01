#include "BigQ.h"
#include <cstdlib>
#include "CustomComparator.h"
#include "CustomComparator2.h"

using namespace std;

vector<Record *> recordArray; //list of  pointer to a Record
vector<Page *> pageArray;     //list of pointer to a Page
map<int, Page *> overflow;

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    //will be called from the main thread itself, not creating another thread
    int pageCount = 0;                          // using to keep track of the number of pages, max could be runlen
    char temp_file[100];
    sprintf(temp_file, "temp%d.bin", rand());
    f.Open(0, temp_file);                //Tempfile for maintaining sorted run
    currentPage = new Page();

    while (true) {  // keep reading from the input buffer until it is empty
        if (in.Remove(&rec) && pageCount < runlen) {    //if buffer has records and pageCount less than runlen, append records to Page
            if (!currentPage->Append(&rec)) {
                pageArray.push_back(currentPage); // push to pageArray since the current page is full.
                currentPage = new Page();         // create new Page instance
                currentPage->Append(&rec);        // append record
                pageCount++;                      // increment pageCount by 1
            }
        } else {
            //input buffer is empty, so sort the records in the pages
            if (pageCount < runlen && currentPage->getRecordCount()) {
                pageArray.push_back(currentPage);    // if the last page has records, add it to the pageArray
            }

            //sort all the records of pages to generate runs
            generateRuns(sortorder);    // stored in recordArray

            //Writing each run into file instance
            writeRunsToFile(runlen);

            //Appending the record of next-run into buffer page, and setting run-page pageCount to 0
            if (pageCount >= runlen) {
                currentPage->Append(&rec);
                pageCount = 0;
                continue;
            } else {
                break;
            }
        }
    } // while loop ends


    //start run merge
    off_t fileLength = f.GetLength();
    off_t numRuns = 0;
    if (fileLength != 0)
        numRuns = ((ceil)((float) (fileLength - 1) / runlen));
    else numRuns = 0;
    off_t last_run_pages = (fileLength - 1) - (numRuns - 1) * runlen;

    priority_queue<Record *, vector<Record *>, CustomComparator2> pq(&sortorder);

    //Maps Record to run (for k-way merge)
    map<Record *, int> m_record;

    //Maintains page numbers of each run
    int *page_index = new int[numRuns];

    //Maintains current buffer page in each run
    Page **page_array = new Page *[numRuns];

    //Load first pages into the memory buffer and first record into priority queue
    int page_num = 0;

    for (int i = 0; i < numRuns; i++) {
        page_array[i] = new Page();
        f.GetPage(page_array[i], page_num);
        page_index[i] = 1;
        Record *r = new Record;
        page_array[i]->GetFirst(r);
        pq.push(r);
        m_record[r] = i;
        r = NULL;
        page_num += runlen;
    }

    //Extract from priority queue and place on the output pipe
    while (!pq.empty()) {

        Record *r = pq.top(); //Extract the min priority record
        pq.pop();

        //run number of record being pushed, -1 sentinel
        int next_rec_run = -1;

        next_rec_run = m_record[r];
        m_record.erase(r);

        if (next_rec_run == -1)
            return;


        Record *next = new Record;
        bool is_rec_found = true;
        if (!page_array[next_rec_run]->GetFirst(next)) {
            //Check if not the end of run
            if ((!(next_rec_run == (numRuns - 1)) && page_index[next_rec_run] < runlen)
                || ((next_rec_run == (numRuns - 1) && page_index[next_rec_run] < last_run_pages))) {

                f.GetPage(page_array[next_rec_run], page_index[next_rec_run] + next_rec_run * runlen);
                page_array[next_rec_run]->GetFirst(next);
                page_index[next_rec_run]++;
            } else {  //Check for overflow page incase of last run
                if (page_index[next_rec_run] == runlen) {
                    if (overflow[(next_rec_run + 1) * runlen - 1]) {
                        delete page_array[next_rec_run];
                        page_array[next_rec_run] = NULL;
                        page_array[next_rec_run] = (overflow[(next_rec_run + 1) * runlen - 1]);
                        overflow[(next_rec_run + 1) * runlen - 1] = NULL;
                        page_array[next_rec_run]->GetFirst(next);
                    } else is_rec_found = false;
                } else is_rec_found = false;
            }
        }
        //Push the next record into the priority queue if found
        if (is_rec_found) {
            pq.push(next);
        }
        m_record[next] = next_rec_run;
        out.Insert(r);
    }
    f.Close();
    out.ShutDown();
}

BigQ::~BigQ() {}

void BigQ::generateRuns(OrderMaker &sortorder) {
    for (int i = 0; i < pageArray.size(); i++) {
        Record *temp = new Record();
        while (pageArray[i]->GetFirst(temp)) {
            recordArray.push_back(temp);
            temp = new Record();
        }
        delete temp;
        delete pageArray[i];
        pageArray[i] = NULL;
        temp = NULL;
    }
    //internal sort all the records using stdlib
    std::sort(recordArray.begin(), recordArray.end(), CustomComparator(&sortorder));

    Schema mySchema("/Users/vaibhav/Downloads/a2test/catalog", "lineitem");
}

void BigQ::writeRunsToFile(int runlen){
    int pageIndex = 0;
    Page *runPage = new Page();
    int pageCount1 = 0;
    for (int i = 0; i < recordArray.size(); i++) {  //traverse through all the records and put them in pages and then into file
        if (!runPage->Append(recordArray[i])) {     //page is full
            pageIndex++;                            //increment the index
            f.AddPage(runPage, pageCount1++);       //add to file
            runPage->EmptyItOut();                  //clear the page
            runPage->Append(recordArray[i]);        //add the next record as first in the empty page
        }
        delete recordArray[i];
    }

    if (pageIndex < runlen) {
        if (runPage->getRecordCount())
            f.AddPage(runPage, pageCount1++);
    } else { //Check for overflow page and map the run number to it
        if (runPage->getRecordCount()) {
            overflow[pageCount1 - 1] = runPage;
        }
    }
    recordArray.clear();
    pageArray.clear();
}