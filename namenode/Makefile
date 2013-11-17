include ../Makefile.common

OBJECTS=dfs_namenode.o

all:$(OBJECTS)
	$(LD) $(OBJECTS) $(LDFLAGS) -o namenode -pthread ../build/libdfscommon.a
	cp namenode ../build;

$(OBJECTS):%.o:%.c
	$(CC) $(CC_FLAGS) -c $< -o $@

clean:
	rm -f *.o
	rm -f namenode 

