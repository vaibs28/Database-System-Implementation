//
// Created by Vaibhav Sahay on 11/02/20.
//

#include "Record.h"

#ifndef DBI_MASTER_CUSTOMCOMPARATOR_H
#define DBI_MASTER_CUSTOMCOMPARATOR_H

#endif //DBI_MASTER_CUSTOMCOMPARATOR_H

class CustomComparator {

private:

    OrderMaker *order;

public:

    CustomComparator(){}

    CustomComparator(OrderMaker *order) {
        this->order = order;
    }

    bool operator()(Record *left, Record *right)  {
        ComparisonEngine ce;
        if (ce.Compare(left, right, this->order) < 0) {
            return true;
        } else {
            return false;
        }
    }

};