include ../Makefile.common

CC_FLAGS += -I../include

OBJECTS=ext.o dfs_datanode.o

all:$(OBJECTS)
	$(LD) $(LDFLAGS) $(OBJECTS) -o datanode -pthread ../build/libdfscommon.a
	cp -v datanode ../build/

witha2:
	gcc $(CC_FLAGS) dfs_datanode.c exta2.c ../a2/fs.c ../a2/ext.c -g -o datanode -pthread ../build/libdfscommon.a
	cp -v datanode ../build/

$(OBJECTS):%.o:%.c
	$(CC) $(CC_FLAGS) -c $< -o $@

clean:
	rm -f *.o *.a
	rm -f datanode 

