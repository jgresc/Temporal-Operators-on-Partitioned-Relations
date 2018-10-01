CC = gcc
CFLAGS  =  -I include -Wall -ggdb
LDFLAGS = -lm 

OBJ = main.o timing.o
	
all: $(OBJ)
	$(CC) $(CFLAGS) -o main $(OBJ) $(LDFLAGS)
	
%.o: %.c %.h
	$(CC) $(CFLAGS) -c $<
	 
clean:
	rm -rf *.o main
