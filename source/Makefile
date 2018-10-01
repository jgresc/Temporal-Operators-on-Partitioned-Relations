CC = gcc
CFLAGS  =  -I include -Wall -ggdb 
LDFLAGS = -lm  

OBJ = main.o  glob.o list.o stack.o OIP.o RIT.o QTREE.o SEGTREE.o SeqScan.o rel.o ttup.o timing.o btree.o File.o MemFile.o AsyncDiskFile.o red_black_tree.o Buffer.o nodeHeapAm.o nodePartitionAm.o nodeSort.o partition.o
	
all: $(OBJ)
	$(CC) $(CFLAGS) -o main $(OBJ) $(LDFLAGS)
	
%.o: %.c %.h
	$(CC) $(CFLAGS) -c $<
	 
clean:
	rm -rf *.o main
