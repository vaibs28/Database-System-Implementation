#include <iostream>
#include "Statistics.h"
#include "Ddl.h"
#include "ParseTree.h"
#include <stdlib.h>
#include "QueryPlanner.h"
using namespace std;

// yyparese method is defined in y.tab.c
extern "C" {
int yyparse(void);
}

// the list of tables and aliases in the query
extern struct TableList *tables;
// the predicate in the WHERE clause
extern struct AndList *boolean;
// grouping atts (NULL if no grouping)
extern struct NameList *groupingAtts;
// the set of attributes in the SELECT (NULL if no such atts)
extern struct NameList *attsToSelect;
// if there is a distinct in non-aggregate query then this value will be 1
extern int distinctAtts;
// if there is a distinct in aggregate query then this value will be 1
extern int distinctFunc;
extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct FuncOperator *finalFunction;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema
extern char* exitflag;
char* filename;

void clear() {
    newattrs = NULL;
    finalFunction = NULL;
    tables = NULL;
    boolean = NULL;
    groupingAtts = NULL;
    attsToSelect = NULL;
    newtable = oldtable = newfile = deoutput = NULL;
    distinctAtts = distinctFunc = 0;
}

int main() {
    char *fileName = "Statistics.txt";
    Statistics s;
    Ddl d;
    //QueryPlan plan(&s);
    QueryPlanner qp;
    QueryNode* root = new QueryNode();
    qp.initStats(&s);
    while(true) {
        cout << "sql(enter quit; to exit) > ";
        yyparse();
        s.Read(fileName);
        if(exitflag){
            exit(0);
        }
        if (newtable) {
            if(oldtable){
                if (d.dropTable())
                    cout << "Dropped table  " << oldtable << endl;
                else
                    cout << "Table " << oldtable << " does not exist." << endl;
                continue;
            }
            bool retVal = d.createTable();
            if (retVal)
                cout << "Created table " << newtable << endl;
            else
                cout << "Table " << newtable << " already exists." <<endl;
        }else if (oldtable && newfile) {
            if (d.insertInto())
                cout << "Inserted into " << oldtable << endl;
            else
                cout << "Insert failed." << endl;
            newfile = NULL;
        }else if (oldtable && !newfile) {
            if (d.dropTable())
                cout << "Dropped table " << oldtable << endl;
            else
                cout << "Table " << oldtable << " does not exist." << endl;
            oldtable = NULL;
        }
        else if(deoutput){
            cout<<"set output"<<endl;
            filename = deoutput;
        }
        else if (tables) {
            if(filename==NULL){
                root = qp.generate(&s,false);
                qp.execute(root, filename);
            }
            else if(strcmp(filename,"NONE")==0){
                root = qp.generate(&s,true);
            }else {
                root = qp.generate(&s, false);
                qp.execute(root, filename);
            }
        }
        clear();
    }
    return 0;
}


