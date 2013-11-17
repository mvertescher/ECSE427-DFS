include ../Makefile.common

OBJECTS=dfs_common.o 

all:$(OBJECTS)
	ar rcs libdfscommon.a $(OBJECTS) 
	cp libdfscommon.a ../build/

$(OBJECTS):%.o:%.c
	$(CC) $(CC_FLAGS) -c $< -o $@

clean:
	rm -f *.o
	rm -f *.a
