# Temporal Operators on Partitioned Relations

## Overview
Having an outer and an inner relation, many operators (i.e., joins, anti-joins and aggregation) can
be computed. By applying a timestamp `T = [ Ts, Te)` on operators, we refer to them as temporal
operators. Our main goal is to compute these temporal operators efficiently. This project addresses the
problem of inefficient temporal operations because of overlapping tuples in each relation. Therefore, 
the relations are split into partitions, in which no tuple overlaps. After this, the operators can be applied more efficiently.


## Disclaimer
My contribution to this project is mostly found in the `main.c` file starting at line 795. 

## Getting Started

### Prerequisites

- [Cygwin](https://www.cygwin.com/) (Windows)
- Make package (Windows)


###  Build & development


1. Clone this repo
   
2. Open Cygwin and cd into `../source`
   
3. run `make all`
   
4. After changing the code, run `make` again


### How to run?
After a successful build, there are several options:


* For a temporal join on disk, use the command:

`./main.exe -o 2 --of tfdb1000k.txt --if tfdb1000k.txt -a DipDisk --on-disk --Join --p 6`

* For a temporal anti-join on disk, use the command:

`./main.exe -o 2 --of tfdb1000k.txt --if tfdb1000k.txt -a DipDisk --on-disk --AntiJoin --p 6`

* For a temporal aggregation on disk, use the command:

`./main.exe -o 2 --of tfdb1000k.txt --if tfdb1000k.txt -a DipDisk --on-disk --Aggregation`

* For a temporal join on memory, use the command:

`./main.exe -o 2 --of tfdb1000k.txt --if tfdb1000k.txt -a DipMem --on-array --Join --p 6`

* For a temporal anti-join on memory, use the command:

`./main.exe -o 2 --of tfdb1000k.txt --if tfdb1000k.txt -a DipMem --on-array --AntiJoin --p 6`

* For a temporal aggregation on memory, use the command:

`./main.exe -o 2 --of tfdb1000k.txt --if tfdb1000k.txt -a DipMem --Aggregation`


The file `tfdb1000k.txt` contains 1000k tuples (start & end time) which randomly overlap each other. The partitions are stored in the respective folder `Partitions1` & `Partition2`.
Two tupel are overlapping if:

`t1.start <= t2.start && t1.end > t2.start`

or

`t1.start < t2.end && t1.end >= t2.end`
