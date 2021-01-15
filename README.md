# Java Minibase Project LAB 3

## Introduction
The main objective of this lab is to play with joins in a DBMS.

- This project is used as part of the DBSys course, of the Data Science branch of [EURECOM](https://eurecom.fr)

- Contributors : Group C : **Julien THOMAS (julien.thomas@eurecom.fr) and Eliot CALIMEZ (eliot.calimez@eurecom.fr)**

- Repository : https://github.com/korrigans84/DBsys-LAB3


## Explore the project
Files created during the project : 

- tests/Lab3Test.java

- tests/QueryFromFile.java

- iterator/IEJoin.java

- iterator/SelfJoinSinglePredicate.java

- iterator/SelfJoin.java

- iterator/IEJoin.java

## Run instructions

You have to change the DATA_DIR_PATH variable of JoinLab3Driver class (in Lab3Test.java file) with your own path for outputs.  
The directory must have 2 diretories : output and csv, and must have q.txt file  
set WRITE_TO_CSV to true if you want to put query informations in csv file.
The csv retrieves the input number of the query, its execution time, as well as its number of output data.  


Now, uncomment the queries that you want test in runTests method (Lab3Test.java, line 72), with your input files.

And run your code

## Task 1a - Test/extend the existing NLJ with single predicate inequality joins.
Successfully implemented
## Task 1b - Test/extend NLJ with two predicates inequality join
Successfully implemented
## Task 2a - Implement single predicate self join operation.
Successfully implemented
## Task 2b - Extend the single predicate inequality self join (Task 2a) to two predicates in the condition of the inequality self join.
Successfully implemented
## Tast 2c - Implement two predicates inequality join
Implemented with limitations or bugs ( Explanations in the report )
