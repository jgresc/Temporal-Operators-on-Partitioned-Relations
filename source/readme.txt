
./main -o 2  --of  /Users/francescocafagna/Desktop/partitioning/tfdb100kLONG.txt  --if /Users/francescocafagna/Desktop/partitioning/tfdb100kLONG.txt  -a DIP --ordered


CREATE PARTITIONS FROM RANDOM DATA
./main -o 1 --on 100000 --ous 1 --oue 1000000 --ol 10000 --in 1000000  --ius 1 --iue 10000000 --il 500000 --of /home/cafagna/partitioning/tfdb.txt --if /Users/gast/Desktop/partitioning/s.txt  -a partition --on-disk

CREATE PARTITIONS FROM FILE
./main -o 2  --of  /Users/francescocafagna/Desktop/partitioning/tfdb100kLONG.txt  --if /Users/francescocafagna/Desktop/partitioning/tfdb100kLONG.txt  -a DIP --ordered


READFILE
./main -o 2 --on 300000 --ous 1 --oue 1000000 --ol 365 --in 10000  --ius 1 --iue 10000 --il 365 --of /Users/gast/Desktop/partitioning/s.txt.part2.txt --if /Users/gast/Desktop/partitioning/tfdb.txt  -a readFile --on-disk

./main -o 2  --of  /media/sf_sf_Vboxshare/Users/Jerinas/Documents/home/dip/tfdb100kLONG.txt  --if /media/sf_sf_Vboxshare/Users/Jerinas/Documents/home/dip//tfdb100kLONG.txt  -a DIP --ordered


CREATE PARTITIONS FROM RANDOM DATA


