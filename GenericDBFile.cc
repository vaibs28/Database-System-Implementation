//
// Created by Vaibhav Sahay on 25/02/20.
//
#include "GenericDBFile.h"

//initializing the static variables to false. Will be used in load method to check if the page is being read by other users.
bool GenericDBFile::isBeingRead = false;


