#!/bin/bash

echo "" > Statistics.txt
./a41test.out 1
cat Statistics.txt > output41.txt
echo "***************************************************************************************************************************************" >> output41.txt

./a41test.out 2
cat Statistics.txt >> output41.txt
echo "***************************************************************************************************************************************" >> output41.txt

./a41test.out 5
cat Statistics.txt >> output41.txt
echo "***************************************************************************************************************************************" >> output41.txt

./a41test.out 10
cat Statistics.txt >> output41.txt
echo "***************************************************************************************************************************************" >> output41.txt

./a41test.out 11
cat Statistics.txt >> output41.txt
echo "***************************************************************************************************************************************" >> output41.txt
