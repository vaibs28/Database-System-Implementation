#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;
DBFile dbf;

void printRecordsFromPages(){
    Schema mySchema ("/Users/vaibhav/Documents/UF CISE/DBI/P1/catalog", "lineitem");
    Page readFromPage;
    //DBFile dbf;
    File file ;
    File* f = dbf.getFile();
    file.GetPage(&readFromPage,1);
    int count = readFromPage.getRecordCount();
    int start = 0;
    while(start<=count-1){
        Record readFromRecord;
        readFromPage.GetFirst(&readFromRecord);
        readFromRecord.Print(&mySchema);
        count--;
    }
    file.Close();
}

int main () {

    // try to parse the CNF
	/*cout << "Enter in your CNF: ";
    	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}*/

    // suck up the schema from the file
    //Schema lineitem ("catalog", "lineitem");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
	//myComparison.GrowFromParseTree (final, &lineitem, literal);

    // print out the comparison to the screen
	//myComparison.Print ();

    // now open up the text file and start procesing it

    FILE *tableFile = fopen ("/Users/vaibhav/Documents/UF_CISE/DBI/P1/table/lineitem.tbl", "r");
    Record temp;
    Schema mySchema ("/Users/vaibhav/Documents/UF CISE/DBI/P1/catalog", "lineitem");


    //File file;
    //Page page;




    dbf.Load(mySchema, "/Users/vaibhav/Documents/UF_CISE/DBI/P1/table/lineitem.tbl");
    printRecordsFromPages();

    //Schema mySchema ("/Users/vaibhav/Documents/UF CISE/DBI/P1/catalog", "lineitem");


    //test for adding records

    while (temp.SuckNextRecord (&mySchema, tableFile) == 1){
        //dbf.Add(temp);
    }

    //print the added records

    //printRecordsFromPages();
    //char *bits = literal.GetBits ();
    //cout << " numbytes in rec " << ((int *) bits)[0] << endl;
    //literal.Print (&supplier);

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    /*int counter = 0;
    ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
        counter++;
        if (counter % 10000 == 0) {
            cerr << counter << "\n";
        }
        if (comp.Compare (&temp, &literal, &myComparison))
                    temp.Print (&mySchema);
        }*/

}


