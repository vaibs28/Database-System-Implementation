#include <fstream>
#include <sstream>
#include "Statistics.h"

Statistics::Statistics() {
}

/**
 *
 * @param copyMe
 */
Statistics::Statistics(Statistics &copyMe) {
    map<string, RelationData *> *ptr = copyMe.GetDbStats();
    map<string, RelationData *>::iterator itr;
    RelationData *tbptr;
    //Iterate over the CopyMe HashMap and copy it over
    for (itr = ptr->begin(); itr != ptr->end(); itr++) {
        tbptr = new RelationData(*itr->second);
        dataMap[itr->first] = tbptr;
    }
}

/**
 *
 */
Statistics::~Statistics() {
    map<string, RelationData *>::iterator itr;
    RelationData *tb = NULL;
    //Iterate over the HashMap and delete the tablestat objects and then clear the HashMap
    for (itr = dataMap.begin(); itr != dataMap.end(); itr++) {
        tb = itr->second;
        delete tb;
        tb = NULL;
    }
    dataMap.clear();
}

/** Add another base relation to the structure to write to Statistics.txt
 *
 * @param relName
 * @param numTuples
 */
void Statistics::AddRel(char *relName, int numTuples) {

    //If the HashMap contains the relation, update the no of tuples, otherwise add new entry
    map<string, RelationData *>::iterator itr;
    RelationData *table;
    itr = dataMap.find(string(relName));
    if (itr != dataMap.end()) {
        //if found update the number
        dataMap[string(relName)]->setNumRows(numTuples);
        dataMap[string(relName)]->setGroupData(relName, 1);
    } else {
        //insert new relation
        table = new RelationData(numTuples, string(relName));
        dataMap[string(relName)] = table;
    }
}

/**
 *
 * @param relName
 * @param attName
 * @param numDistincts
 */
void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    //if the HashMap contains the relation, update the data in RelationData
    map<string, RelationData *>::iterator itr;
    itr = dataMap.find(string(relName));
    if (itr != dataMap.end()) {
        dataMap[string(relName)]->setData(string(attName), numDistincts);
    }
}

/**
 *
 * @param oldName
 * @param newName
 */
void Statistics::CopyRel(char *oldName, char *newName) {
    /*Logic:
    If the HashMap contains the old relation, copy it over to the new Relation and insert new relation into dataMap
    Else report error*/
    string oldRel = string(oldName);
    string newRel = string(newName);
    if (strcmp(oldName, newName) == 0) return;

    map<string, RelationData *>::iterator itr2;
    itr2 = dataMap.find(newRel);
    if (itr2 != dataMap.end()) {
        delete itr2->second;
        string temp = itr2->first;
        itr2++;
        dataMap.erase(temp);

    }

    map<string, RelationData *>::iterator itr;

    itr = dataMap.find(oldRel);
    RelationData *tbptr;

    if (itr != dataMap.end()) {
        RelationData *newTable = new RelationData(dataMap[string(oldName)]->getTupleCount(), newRel);
        tbptr = dataMap[oldRel];
        map<string, int>::iterator tableiter = tbptr->getAttributes()->begin();
        for (; tableiter != tbptr->getAttributes()->end(); tableiter++) {
            string temp = newRel + "." + tableiter->first;
            newTable->setData(temp, tableiter->second);
        }
        dataMap[string(newName)] = newTable;
    } else {
        cout << "\n Class:Statistics Method:CopyRel Msg: invalid relation name:" << oldName << endl;
        exit(1);
    }
}

/**
 *
 * @param fromWhere
 */
void Statistics::Read(char *fromWhere) {
    FILE *fptr = NULL;
    fptr = fopen(fromWhere, "r");
    char strRead[200];
    while (fptr != NULL && fgets(strRead, sizeof strRead,fptr) != NULL) {
        if (strcmp(strRead, "GROUP DETAILS\n") == 0) {
            int tuplecnt = 0;
            char* relname;
            int grpcnt = 0;
            char groupname[200];
            char dummy[200];
            fscanf(fptr, "%s %s %s %s", dummy, dummy, dummy, groupname);
            fscanf(fptr, "%s %s %s %d", dummy, dummy, dummy, &grpcnt);
            fscanf(fptr, "%s %s %s %d", dummy, dummy, dummy, &tuplecnt);
            strcpy(dummy,groupname);
            relname = strtok(dummy,",");
            AddRel(relname, tuplecnt);
            dataMap[string(relname)]->setGroupData(groupname, grpcnt);
            char attname[200];
            int distcnt = 0;
            fscanf(fptr, "%s %s",dummy, dummy);
            int count = 0;
            for(int i=0;i<grpcnt;i++){
                if(count==0)
                    fscanf(fptr, "%s %s %s", dummy, dummy, dummy);
                else
                    fscanf(fptr, "%s %s", dummy, dummy);
                count++;
                int count1 = 0;
                while (strcmp(attname, "Table") != 0) {
                    if(count1==0)
                        fscanf(fptr, "%s", attname);
                    fscanf(fptr, "%d", &distcnt);
                    AddAtt(relname, attname, distcnt);
                    fscanf(fptr, "%s", attname);
                    count1++;
                }
            }


        }
    }
}


/**
 *
 * @param fromWhere
 */
void Statistics::Write(char *fromWhere) {
    map<string, RelationData *>::iterator dbitr;
    map<string, int>::iterator tbitr;
    map<string, int> *attrptr;

    FILE *fptr;
    fptr = fopen(fromWhere, "w");
    dbitr = dataMap.begin();

    int counter = 0;
    for (; dbitr != dataMap.end(); dbitr++) {
        if (counter == 0) {
            fprintf(fptr, "GROUP DETAILS\n");
            fprintf(fptr, "Group Tables = %s\n", dbitr->second->getGroupName().c_str());
            fprintf(fptr, "Group Size = %d\n", dbitr->second->getGroupSize());
            fprintf(fptr, "Rows Count = %ld\n\n", dbitr->second->getTupleCount());
            fprintf(fptr, "TABLE DETAILS\n");
        }

        counter++;
        fprintf(fptr, "Table = %s\n", dbitr->first.c_str());
        fprintf(fptr, "Group = %s\n", dbitr->second->getGroupName().c_str());
        fprintf(fptr,"Rows = %d\n",dbitr->second->getTupleCount());
        attrptr = dbitr->second->getAttributes();
        tbitr = attrptr->begin();

        for (; tbitr != attrptr->end(); tbitr++) {
            fprintf(fptr, "%s %d\n", tbitr->first.c_str(), tbitr->second);
        }

        fprintf(fptr, "\n");
    }
    fprintf(fptr, "**********************************************************");
    fclose(fptr);
}

/**
 * Exactly like Estimate and stores the result in Statistics to write to the file.
 * @param parseTree
 * @param relNames
 * @param numToJoin
 */
void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    double estimateResult = Estimate(parseTree, relNames, numToJoin);

    long result = (long) ((estimateResult - floor(estimateResult)) >= 0.5 ? ceil(estimateResult) : floor(
            estimateResult));
    string groupName = "";
    int groupSize = numToJoin;
    for (int i = 0; i < groupSize; i++) {
        if (i != 0)
            groupName = groupName + "," + relNames[i];
        else
            groupName = relNames[i];
    }
    map<string, RelationData *>::iterator itr = dataMap.begin();
    for (int i = 0; i < numToJoin; i++) {
        dataMap[relNames[i]]->setGroupData(groupName, groupSize);
        dataMap[relNames[i]]->setNumRows(result);
    }
}

/**
 *
 * @param parseTree
 * @param relNames
 * @param numToJoin
 * @return
 */
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
    /*
     * Check validity of input parameters
     * Loop throught the AND LIST.
     * For each node of AND LIST which is OR LIST evaluate the Selectivity.
     * Do Product of all Selectivities and multiply result with the Tuple product of relations
     */

    double estTuples = 1.0;
    map<string, long> uniqvallist;
    if (!Validate(parseTree, relNames, numToJoin, uniqvallist)) {
        cout << "\nClass:Statistics Method:Estimate Msg:Input Parameters invalid for Estimation";
        return -1.0;
    } else {
        string groupName = "";
        map<string, long>::iterator tupitr;
        map<string, long> tuplevals;
        int groupSize = numToJoin;
        for (int i = 0; i < groupSize; i++) {
            groupName = groupName + "," + relNames[i];
        }
        for (int i = 0; i < numToJoin; i++) {
            tuplevals[dataMap[relNames[i]]->getGroupName()] = dataMap[relNames[i]]->getTupleCount();
        }

        estTuples = 1000.0; //Safety purpose so that we dont go out of Double precision
        while (parseTree != NULL) {
            estTuples *= Evaluate(parseTree->left, uniqvallist);
            parseTree = parseTree->rightAnd;
        }
        tupitr = tuplevals.begin();
        for (; tupitr != tuplevals.end(); tupitr++) {
            estTuples *= tupitr->second;
        }
    }
    estTuples = estTuples / 1000.0; //Safety purpose so that we dont go out of Double precision-revert
    return estTuples;
}

/**
 *
 * @param orList
 * @param uniqvallist
 * @return
 */
double Statistics::Evaluate(struct OrList *orList, map<string, long> &uniqvallist) {
    /*Logic:
    LESS_THAN code =  1 , Selectivity = 1/3
    GREATER_THAN code = 2, Selectivity = 1/3
    EQUALS code =3 , Selectivity  = 1 / max(V(R,A),V(S,A))
   */
    struct ComparisonOp *comp;
    map<string, double> attribSelectivity;

    while (orList != NULL) {
        comp = orList->left;
        string key = string(comp->left->value);
        if (attribSelectivity.find(key) == attribSelectivity.end()) {
            attribSelectivity[key] = 0.0;
        }
        if (comp->code == 1 || comp->code == 2)  // check for < and >
        {
            attribSelectivity[key] = attribSelectivity[key] + 1.0 / 3;
        } else {
            string ulkey = string(comp->left->value);
            long max = uniqvallist[ulkey];
            if (comp->right->code == 4)    // for =
            {
                string urkey = string(comp->right->value);
                if (max < uniqvallist[urkey])
                    max = uniqvallist[urkey];
            }
            attribSelectivity[key] = attribSelectivity[key] + 1.0 / max;
        }
        orList = orList->rightOr; //assign next or
    }

    double selectivity = 1.0;
    map<string, double>::iterator itr = attribSelectivity.begin();
    for (; itr != attribSelectivity.end(); itr++)
        selectivity *= (1.0 - itr->second);

    return (1.0 - selectivity);
}

/**
 *
 * @param parseTree
 * @param relNames
 * @param numToJoin
 * @param uniqvallist
 * @return
 */
bool
Statistics::Validate(struct AndList *parseTree, char *relNames[], int numToJoin, map<string, long> &uniqvallist) {
    /*
     Logic:
     * 1.Check if all the attributes in parse tree are part of the
     * relations in RelNames.
     * 2.Check if the all Relnames of partitions used, are tin relNames.
     */
    bool result = true;
    while (parseTree != NULL && result) {
        struct OrList *head = parseTree->left;
        while (head != NULL && result) {
            struct ComparisonOp *ptr = head->left;
            if (ptr->left->code == 4 && ptr->code == 3 &&
                !ContainsAttribute(ptr->left->value, relNames, numToJoin, uniqvallist)) {
                cout << "Error" <<endl;
                result = false;
            }
            if (ptr->right->code == 4 && ptr->code == 3 &&
                !ContainsAttribute(ptr->right->value, relNames, numToJoin, uniqvallist))
                result = false;
            head = head->rightOr;
        }
        parseTree = parseTree->rightAnd;
    }
    if (!result) return result;

    map<string, int> tmpTable;
    for (int i = 0; i < numToJoin; i++) {
        string grpname = dataMap[string(relNames[i])]->getGroupName();
        if (tmpTable.find(grpname) != tmpTable.end())
            tmpTable[grpname]--;
        else
            tmpTable[grpname] = dataMap[string(relNames[i])]->getGroupSize() - 1;
    }

    map<string, int>::iterator tmpTableItr = tmpTable.begin();
    for (; tmpTableItr != tmpTable.end(); tmpTableItr++)
        if (tmpTableItr->second != 0) {
            result = false;
            break;
        }
    return result;
}

/**
 *
 * @param value
 * @param relNames
 * @param numToJoin
 * @param uniqvallist
 * @return
 */
bool Statistics::ContainsAttribute(char *value, char *relNames[], int numToJoin, map<string, long> &uniqvallist) {
    int i = 0;
    while (i < numToJoin) {
        map<string, RelationData *>::iterator itr = dataMap.find(relNames[i]);
        if (itr != dataMap.end()) {   //if found, check for attributes
            string key = string(value);
            if (itr->second->getAttributes()->find(key) != itr->second->getAttributes()->end()) {
                uniqvallist[key] = itr->second->getAttributes()->find(key)->second;
                return true;
            }
        } else
            return false;
        i++;
    }
    return false;
}

void Statistics::ParseRelation(struct Operand* op, string& relation) {

    string value(op->value);
    string reln;
    stringstream s;

    int i = 0;

    while (value[i] != '_') {

        if (value[i] == '.') {
            relation = s.str();
            return;
        }

        s << value[i];

        i++;

    }

}


