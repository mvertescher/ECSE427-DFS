include ../Makefile.common

OBJECTS=dfs_client.o

all:$(OBJECTS)

$(OBJECTS):%.o:%.c
	$(CC) $(CC_FLAGS) -c $< -o $@

clean:
	rm -f *.o
	rm -f dfsclient

