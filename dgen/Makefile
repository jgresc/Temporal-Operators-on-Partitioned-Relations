CC = gcc
CFLAGS  =  -I include -Wall -ggdb
LDFLAGS = -lm

OBJ = dgen.o
	
all: $(OBJ)
	$(CC) $(CFLAGS) -o dgen $(OBJ) $(LDFLAGS)
	
%.o: %.c %.h
	$(CC) $(CFLAGS) -c $<
	 
clean:
	rm -rf *.o dgen