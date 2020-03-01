//
// Created by Vaibhav Sahay on 25/02/20.
//
#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

//initializing the static variables to false. Will be used in load method to check if the page is being read by other users.
bool GenericDBFile::isBeingRead = false;
