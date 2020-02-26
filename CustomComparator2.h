//
// Created by Vaibhav Sahay on 16/02/20.
//

#include "Comparison.h"

#ifndef A2TEST_CUSTOMCOMPARATOR2_H
#define A2TEST_CUSTOMCOMPARATOR2_H

#endif //A2TEST_CUSTOMCOMPARATOR2_H

class CustomComparator2 {
    OrderMaker *order;

public:
    CustomComparator2(OrderMaker *orderMaker) {
        this->order = orderMaker;
    }

    bool operator()(Record *r1, Record *r2) {
        ComparisonEngine comp;
        if (comp.Compare(r1, r2, order) < 0)
            return false;
        else return true;
    }
};