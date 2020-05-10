#include <stdio.h> // remove, move, etc
#include <string>
#include <fstream>
#include "GenericDBFile.h"
#include "DBFile.h"
#include "ParseTree.h"
#include "Comparison.h"
#include "Ddl.h"
#include "Constants.h"

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern char *newtable;
extern char *oldtable;
extern char *newfile;
extern char *deoutput;
extern struct AttrList *newattrs; //Use this to build Attribute* array and record schema
extern struct NameList *sortattrs;

char *catalog_path = CATALOG_PATH;
char *dbfile_dir = DBFILE_PATH;
char *tpch_dir = TPCH_PATH;

bool Ddl::createTable() { // CREATE TABLE
    if (exists(newtable))
        return false;
    fstream fmeta((string(newtable) + ".meta").c_str());
    fType t = (sortattrs ? sorted : heap);
    fmeta << t << endl;
    int numAtts = 0;
    fstream fcat("catalog", std::ios_base::app);
    fcat << "BEGIN\n" << newtable << '\n' << newtable << ".tbl" << endl;
    const char *myTypes[3] = {"Int", "Double", "String"};
    for (AttrList *att = newattrs; att; att = att->next, ++numAtts)
        fcat << att->name << ' ' << myTypes[att->type] << endl;
    fcat << "END" << endl;
    Attribute *atts = new Attribute[numAtts];
    Type types[3] = {Int, Double, String};
    numAtts = 0;
    for (AttrList *att = newattrs; att; att = att->next, numAtts++) {
        atts[numAtts].name = strdup(att->name);
        atts[numAtts].myType = types[att->type];
    }
    Schema newSchema("", numAtts, atts);
    OrderMaker sortOrder;
    if (sortattrs) {
        sortOrder.growFromParseTree(sortattrs, &newSchema);
        sortOrder.WriteToMetaFile(fcat);
        fmeta << 512 << endl;
    }

    struct SortInfo {
        OrderMaker *myOrder;
        int runLength;
    } info = {&sortOrder, 256};
    DBFile newTable;
    newTable.Create((char *) (std::string(newtable) + ".bin").c_str(), t, (void *) &info); // create ".bin" files
    newTable.Close();

    delete[] atts;
    fmeta.close();
    fcat.close();
    return true;
}

bool Ddl::insertInto() { // INSERT INTO
    DBFile table;
    char *fpath = new char[strlen(oldtable) + 4];
    strcpy(fpath, oldtable);
    strcat(fpath, ".bin");
    Schema sch(catalog_path, oldtable);
    if (table.Open(fpath)) {
        table.Load(sch, newfile);
        table.Close();
        delete[] fpath;
        return true;
    }
    delete[] fpath;
    return false;
}

bool Ddl::dropTable() { // DROP TABLE
    // delete from catalog
    string schString = "", line = "", relName = oldtable;
    ifstream fin(catalog_path);
    ofstream fout(".tmp");
    bool found = false, exists = false;
    while (getline(fin, line)) {
        if (trim(line).empty()) continue;
        if (line == oldtable) exists = found = true;
        schString += trim(line) + '\n';
        if (line == "END") {
            if (!found) fout << schString << endl;
            found = false;
            schString.clear();
        }
    }

    rename(".tmp", catalog_path);
    fin.close();
    fout.close();

    // delete bin, meta
    if (exists) {
        remove((relName + ".bin").c_str());
        remove((relName + ".bin.meta").c_str());
        return true;
    }
    return false;
}

bool Ddl::exists(const char *relName) {
    ifstream fin(catalog_path);
    string line;
    while (getline(fin, line))
        if (trim(line) == relName) {
            fin.close();
            return true;
        }
    fin.close();
    return false;
}
