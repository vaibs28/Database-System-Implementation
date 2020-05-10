
#include "QueryPlanner.h"

using namespace std;
extern "C" {
int yyparse(void);
}

extern struct TableList *tables;
extern struct FuncOperator *finalFunction;
extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern int distinctFunc;
extern int distinctAtts;
extern struct NameList *attsToSelect;

int QueryPlanner::initStats(Statistics *stats) {
    char *relName[] = {"supplier", "partsupp", "lineitem", "orders", "customer", "nation", "region", "part"};


    stats->AddRel(relName[0], 10000);
    stats->AddAtt(relName[0], "s_suppkey", 10000);
    stats->AddAtt(relName[0], "s_name", 10000);
    stats->AddAtt(relName[0], "s_address", 10000);
    stats->AddAtt(relName[0], "s_nationkey", 25);
    stats->AddAtt(relName[0], "s_phone", 10000);
    stats->AddAtt(relName[0], "s_acctbal", 9955);
    stats->AddAtt(relName[0], "s_comment", 10000);


    stats->AddRel(relName[1], 800000);
    stats->AddAtt(relName[1], "ps_partkey", 200000);
    stats->AddAtt(relName[1], "ps_suppkey", 10000);
    stats->AddAtt(relName[1], "ps_availqty", 9999);
    stats->AddAtt(relName[1], "ps_supplycost", 99865);
    stats->AddAtt(relName[1], "ps_comment", 799123);


    stats->AddRel(relName[2], 6001215);
    stats->AddAtt(relName[2], "l_orderkey", 1500000);
    stats->AddAtt(relName[2], "l_partkey", 200000);
    stats->AddAtt(relName[2], "l_suppkey", 10000);
    stats->AddAtt(relName[2], "l_linenumber", 7);
    stats->AddAtt(relName[2], "l_quantity", 50);
    stats->AddAtt(relName[2], "l_extendedprice", 933900);
    stats->AddAtt(relName[2], "l_discount", 11);
    stats->AddAtt(relName[2], "l_tax", 9);
    stats->AddAtt(relName[2], "l_returnflag", 3);
    stats->AddAtt(relName[2], "l_linestatus", 2);
    stats->AddAtt(relName[2], "l_shipdate", 2526);
    stats->AddAtt(relName[2], "l_commitdate", 2466);
    stats->AddAtt(relName[2], "l_receiptdate", 2554);
    stats->AddAtt(relName[2], "l_shipinstruct", 4);
    stats->AddAtt(relName[2], "l_shipmode", 7);
    stats->AddAtt(relName[2], "l_comment", 4501941);

    stats->AddRel(relName[3], 1500000);
    stats->AddAtt(relName[3], "o_orderkey", 1500000);
    stats->AddAtt(relName[3], "o_custkey", 99996);
    stats->AddAtt(relName[3], "o_orderstatus", 3);
    stats->AddAtt(relName[3], "o_totalprice", 1464556);
    stats->AddAtt(relName[3], "o_orderdate", 2406);
    stats->AddAtt(relName[3], "o_orderpriority", 5);
    stats->AddAtt(relName[3], "o_clerk", 1000);
    stats->AddAtt(relName[3], "o_shippriority", 1);
    stats->AddAtt(relName[3], "o_comment", 1478684);


    stats->AddRel(relName[4], 150000);
    stats->AddAtt(relName[4], "c_custkey", 150000);
    stats->AddAtt(relName[4], "c_name", 150000);
    stats->AddAtt(relName[4], "c_address", 150000);
    stats->AddAtt(relName[4], "c_nationkey", 25);
    stats->AddAtt(relName[4], "c_phone", 150000);
    stats->AddAtt(relName[4], "c_acctbal", 140187);
    stats->AddAtt(relName[4], "c_mktsegment", 5);
    stats->AddAtt(relName[4], "c_comment", 149965);

    stats->AddRel(relName[5], 25);
    stats->AddAtt(relName[5], "n_nationkey", 25);
    stats->AddAtt(relName[5], "n_name", 25);
    stats->AddAtt(relName[5], "n_regionkey", 5);
    stats->AddAtt(relName[5], "n_comment", 25);

    stats->AddRel(relName[6], 25);
    stats->AddAtt(relName[6], "r_regionkey", 5);
    stats->AddAtt(relName[6], "r_name", 5);
    stats->AddAtt(relName[6], "r_comment", 5);


    stats->AddRel(relName[7], 200000);
    stats->AddAtt(relName[7], "p_partkey", 200000);
    stats->AddAtt(relName[7], "p_name", 199996);
    stats->AddAtt(relName[7], "p_mfgr", 5);
    stats->AddAtt(relName[7], "p_brand", 25);
    stats->AddAtt(relName[7], "p_type", 150);
    stats->AddAtt(relName[7], "p_size", 50);
    stats->AddAtt(relName[7], "p_container", 40);
    stats->AddAtt(relName[7], "p_retailprice", 20899);
    stats->AddAtt(relName[7], "p_comment", 127459);

    return 1;
}

vector<AndList> QueryPlanner::DetermineJoinOrder(Statistics *stats, vector<AndList> list) {
    AndList boolean;
    vector<AndList> curr;
    curr.reserve(list.size());
    string s1, s2;
    double estimate = 0.0, min = -1.0;
    int x = 0, count = 0, minIdx = 0;
    vector<string> temp;

    while (1 < list.size()) {
        while (list.size() > x) {
            boolean = list[x];
            stats->ParseRelation(boolean.left->left->left, s1);
            stats->ParseRelation(boolean.left->left->right, s2);
            char *relations[] = {(char *) s1.c_str(), (char *) s2.c_str()};
            if (min != -1.0) {
                estimate = stats->Estimate(&boolean, relations, 2);
                if (min > estimate) {
                    minIdx = x;
                    min = estimate;
                }
            } else {
                minIdx = x;
                min = stats->Estimate(&boolean, relations, 2);
            }
            x++;
        }

        temp.push_back(s1);
        temp.push_back(s2);
        curr.push_back(list[minIdx]);

        min = -1.0;
        x = 0;
        count++;
        list.erase(list.begin() + minIdx);
    }
    curr.insert(curr.begin() + count, list[0]);
    return curr;
}

void QueryPlanner::joinOperation(Statistics stats, vector<AndList> &links, vector<AndList> &join, vector<AndList> &pick) {
    struct OrList *curr;
    while (boolean != 0) {
        curr = boolean->left;
        if (curr && curr->left->code == EQUALS && curr->left->left->code == NAME && curr->left->right->code == NAME) {
            AndList currAnd = *boolean;
            currAnd.rightAnd = 0;
            links.push_back(currAnd);
        } else {
            curr = boolean->left;
            if (curr->left == 0) {
                AndList currAnd = *boolean;
                currAnd.rightAnd = 0;
                pick.push_back(currAnd);
            } else {
                vector<string> tab;
                while (curr != 0) {
                    Operand *opd = curr->left->left;
                    if (opd->code != NAME)
                        opd = curr->left->right;
                    string s;
                    stats.ParseRelation(opd, s);
                    if (tab.size() == 0)
                        tab.push_back(s);
                    else if (s.compare(tab[0]) != 0)
                        tab.push_back(s);
                    curr = curr->rightOr;
                }

                if (1 < tab.size()) {
                    AndList currAnd = *boolean;
                    currAnd.rightAnd = 0;
                    join.push_back(currAnd);
                } else {
                    AndList currAnd = *boolean;
                    currAnd.rightAnd = 0;
                    pick.push_back(currAnd);
                }
            }
        }
        boolean = boolean->rightAnd;
    }
}

QueryNode* QueryPlanner::generate(Statistics *stats, bool printFlag){
    QueryNode qt;
    int counter = 0;
    vector<string> rows;
    string initiate;
    vector<AndList> selectList;
    vector<AndList> joinList;
    vector<AndList> join;

    joinOperation(*stats, joinList, join, selectList);
    cout << endl;
    if(printFlag){
        cout << "Number of selects: " << selectList.size() << endl;
        cout << "Number of joins: " << joinList.size() << endl;
    }
    TableList *nameTab = tables;
    map<string, QueryNode *> relToNodeMap;

    QueryNode *rootNode = NULL;
    QueryNode *iterableNode;
    QueryNode *highOrderNode = NULL;
    while (nameTab != 0) {
        switch (nameTab->aliasAs == 0) {
            case true:
                relToNodeMap.insert(std::pair<string, QueryNode *>(nameTab->tableName, new QueryNode()));
                break;
            default:
                relToNodeMap.insert(std::pair<string, QueryNode *>(nameTab->aliasAs, new QueryNode()));
                stats->CopyRel(nameTab->tableName, nameTab->aliasAs);
        }
        rootNode = relToNodeMap[nameTab->aliasAs];
        rootNode->schema = new Schema("catalog", nameTab->tableName);

        if (nameTab->aliasAs != 0) {
            rootNode->schema->updateName(string(nameTab->aliasAs));
        }

        highOrderNode = rootNode;
        rootNode->outputPipe = counter++;

        string strr(nameTab->tableName);

        rootNode->SetType(SELECTFILE);
        //adding for assgn 5

        Record *literal = new Record();
        if(rootNode->cnf != NULL)
            rootNode->opCNF->GrowFromParseTree(rootNode->cnf, rootNode->schema, *literal);
        rootNode->sf = new SelectFile();
        rootNode->db = new DBFile();
        rootNode->tablename = nameTab->tableName;
        rootNode->output = new Pipe(100);
        rootNode->rec = literal;
        nameTab = nameTab->next;
    }
    string attb;
    string tab;
    unsigned it = 0;
    AndList selIterator;

    while (it < selectList.size()) {
        selIterator = selectList[it];

        switch (selIterator.left->left->left->code != NAME) {
            case true:
                stats->ParseRelation(selIterator.left->left->right, tab);
                break;
            default:
                stats->ParseRelation(selIterator.left->left->left, tab);
        }

        iterableNode = relToNodeMap[tab];
        iterableNode->cnf = &selectList[it];
        Record *literal = new Record();
        iterableNode->opCNF = new CNF();
        iterableNode->opCNF->GrowFromParseTree(iterableNode->cnf, iterableNode->schema, *literal);
        iterableNode->rec = literal;

        initiate = tab;
        while (iterableNode->root != NULL) {
            iterableNode = iterableNode->root;
        }
        rootNode = new QueryNode();

        //SELECT PIPE
        qt.setAttr(rootNode, iterableNode);

        rootNode->cnf = &selectList[it];
        literal = new Record();
        rootNode->opCNF = new CNF();
        rootNode->opCNF->GrowFromParseTree(rootNode->cnf, rootNode->schema, *literal);
        rootNode->leftChildPipe = iterableNode->outputPipe;
        rootNode->outputPipe = counter++;
        rootNode->rec = literal;

        char *sApp = strdup(tab.c_str());

        stats->Apply(&selIterator, &sApp, 1);

        highOrderNode = rootNode;
        it += 1;
    }

    if (joinList.size() > 1) {
        joinList = DetermineJoinOrder(stats, joinList);
    }

    string s1 = "";
    AndList currJ;
    QueryNode *leftTNode;
    string s2 = "";
    QueryNode *rightTNode;

    unsigned j = 0;

    while (j < joinList.size()) {
        currJ = joinList[j];

        stats->ParseRelation(currJ.left->left->left, s1);

        stats->ParseRelation(currJ.left->left->right, s2);

        tab = s1;
        leftTNode = relToNodeMap[s1];
        rightTNode = relToNodeMap[s2];

        while (leftTNode->root != NULL) {
            leftTNode = leftTNode->root;
        }

        while (rightTNode->root != NULL) {
            rightTNode = rightTNode->root;
        }

        rootNode = new QueryNode();
        //join setting variables
        rootNode->type = JOIN;
        Record *literal = new Record();
        rootNode->leftChildPipe = leftTNode->outputPipe;
        rootNode->rightChildPipe = rightTNode->outputPipe;
        rootNode->outputPipe = counter++;
        rootNode->cnf = &joinList[j];
        rootNode->opCNF = new CNF();
        rootNode->opCNF->GrowFromParseTree(rootNode->cnf, leftTNode->schema, rightTNode->schema, *literal);
        rootNode->tempCNFForJoins = rootNode->cnf;

        qt.setVal(rootNode, leftTNode, rightTNode, highOrderNode);

        rootNode->CreateSchema();

        highOrderNode = rootNode;
        j++;
    }

    int i = 0;

    while (i < join.size()) {
        iterableNode = highOrderNode;
        rootNode = new QueryNode();

        qt.setAttr(rootNode, iterableNode);

        rootNode->cnf = &join[i];
        rootNode->leftChildPipe = iterableNode->outputPipe;

        rootNode->outputPipe = counter++;

        highOrderNode = rootNode;
        i++;
    }

    if (finalFunction != 0) {
        if (distinctFunc != 0) {
            rootNode = new QueryNode();
            rootNode->type = DISTINCT;

            rootNode->left = highOrderNode;
            rootNode->leftChildPipe = highOrderNode->outputPipe;

            rootNode->outputPipe = counter++;
            rootNode->schema = highOrderNode->schema;

            highOrderNode->root = rootNode;
            highOrderNode = rootNode;
        }

        switch (groupingAtts == 0) {
            case true:
                // SUM
                rootNode = new QueryNode();
                qt.setSum(rootNode, highOrderNode);
                rootNode->outputPipe = counter++;
                rootNode->distinct = distinctFunc;
                rootNode->funcOp = finalFunction;
                rootNode->f = new Function();
                rootNode->f->GrowFromParseTree(finalFunction, *highOrderNode->schema);
                rootNode->schema = highOrderNode->schema;

                rootNode->CreateFunction();
                break;

            default:
                //GROUP BY
                rootNode = new QueryNode();
                qt.setGrpBy(rootNode, highOrderNode);
                rootNode->outputPipe = counter++;
                rootNode->schema = highOrderNode->schema;
                rootNode->distinct = distinctFunc;
                int numOfAttributes;
                vector<int> typesOfAttributes;
                numOfAttributes = 0;
                vector<int> whichAttributes;
                NameList *grpIterate = groupingAtts;
                while (grpIterate) {
                    numOfAttributes = numOfAttributes + 1;
                    whichAttributes.push_back(rootNode->schema->Find(grpIterate->name));
                    typesOfAttributes.push_back(rootNode->schema->FindType(grpIterate->name));
                    if(printFlag) {
                        cout << "GROUPING ON ";
                        cout << grpIterate->name;
                        cout << endl;
                    }
                    grpIterate = grpIterate->next;
                }

                qt.setGrpAtt(rootNode, numOfAttributes, typesOfAttributes, whichAttributes);
        }
    }
    highOrderNode = rootNode;

    if (distinctAtts != 0) {
        //DISTINCT
        rootNode = new QueryNode();
        qt.setDistinct(rootNode, highOrderNode);
        rootNode->outputPipe = counter++;
        rootNode->schema = highOrderNode->schema;
        highOrderNode = rootNode;
    }

    if (attsToSelect != NULL) {
        //PROJECT
        iterableNode = highOrderNode;
        rootNode = new QueryNode();
        Schema *inputSchema = highOrderNode->schema;
        qt.setProject(rootNode, iterableNode, highOrderNode);
        rootNode->outputPipe = counter++;
        qt.attSet(iterableNode, rootNode, printFlag);
        vector<int> *keepMeVector = new vector<int>;
        Schema *outputSchema = new Schema(inputSchema, attsToSelect, keepMeVector);
        int *keepMe = new int();
        keepMe = &keepMeVector->at(0);
        rootNode->atttributesToKeep = keepMe;
        rootNode->noAttsIn = inputSchema->GetNumAtts();
        rootNode->noAttsOut = outputSchema->GetNumAtts();

    };
    if(printFlag)
        print(rootNode);
    return rootNode;

}

void QueryPlanner::print(QueryNode* rootNode) {
    cout << "PRINTING TREE IN ORDER: ";
    cout << endl;
    cout << endl;

    if (rootNode!=NULL) {
        rootNode->postOrder();
    }
}

void PostOrderRun(QueryNode* currentNode){
    if(currentNode==NULL){
        return;
    }
    PostOrderRun(currentNode->left);
    PostOrderRun(currentNode->right);
    currentNode->execute();
}

void PostOrderWait(QueryNode* currentNode){
    if(!currentNode){
        return;
    }
    PostOrderWait(currentNode->left);
    PostOrderWait(currentNode->right);
    //currentNode->waitUntilDone();
}

streambuf * buf= std::cout.rdbuf();
ofstream of;
ostream out(buf);

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
    Record rec;
    int cnt = 0;
    cout << "\n";
    //in_pipe.done = 1;

    while (in_pipe.Remove (&rec)) {
        if (print) {
            rec.Print (schema);
        }
        cnt++;
    }
    in_pipe.ShutDown();
    return cnt;
}

void QueryPlanner::execute(QueryNode *root, char *outputFlag) {
    if(!root)
        return;
    PostOrderRun(root);
    //if(QueryRoot->output->done!=1)
      //  QueryRoot->output->ShutDown();

    if(root->type == SUM){
        root->schema = &sumSchema;

    }else if(root->type == GROUPBY){
        root->schema = &sumSchema;
    }
    cout<<endl;
    if(outputFlag==NULL || strcmp(outputFlag,"STDOUT")==0) {
        //print on STDOUT
        int cnt = clear_pipe(*root->output, root->schema, true);
        cout << "\nQuery returned " << cnt << " records \n";
    }else{
        WriteOut writeOut;
        FILE *writefile = fopen (outputFlag, "w");
        writeOut.Run(*root->output, writefile, *root->schema);
        writeOut.WaitUntilDone();
    }
}
