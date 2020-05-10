#include <assert.h>
#include <iostream>
#include <map>
#include <string.h>
#include <vector>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlanner.h"

using namespace std;
extern "C" {
int yyparse(void);
}

/**
 * Already Defined in Parsetree.
 * Data Structures to contain the parsed data from the query.
 */
extern struct TableList *tables;
extern struct FuncOperator *finalFunction;
extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern int distinctFunc;
extern int distinctAtts;
extern struct NameList *attsToSelect;



int main() {
    Statistics *stats = new Statistics();
    //initialize stats object
    QueryPlanner qp;
    qp.initStats(stats);
    int counter = 1;
    cout << "enter: ";  //print the prompt for taking in the query
    yyparse();          //parses the query and populates the data

    //generate the query plan
    QueryNode* root = qp.generate(stats);
    //qp.print(root);
}
