## Database System Implementation

* Designed a single-user database management system that supports a subset of SQL. 
* Implemented heap and sorted file organization to manage database records. 
* Added relational algebra operations like select, project, join, sum, group by. 
* Implemented query optimization plan.

### Pre-Requisites
  * To test the database, we are using [tpch-dbgen](https://github.com/electrum/tpch-dbgen.git) as sample data.
  * Clone the [tpch-dbgen](https://github.com/electrum/tpch-dbgen.git) repo to test.
  * Compile it:`make`
  * To generate 10MB data, run `./dbgen -s 0.01`. To generated 1GB data `./dbgen -s 1`.
  * This will generate 8 *.tbl files containing the data in CSV format with | separator

### Steps to Run
* make main
* ./main
* input the queries from tc1 to tc5
