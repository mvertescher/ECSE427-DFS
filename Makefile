include Makefile.common

all:
	rm -rf build;
	mkdir build;
	make -C common;
	make -C datanode;
	make -C namenode;
	make -C client;
	$(CC) -I./include/ -g -c test.c -o test.o
	$(LD) $(LD_FLAGS) client/dfs_client.o test.o -o dfs build/libdfscommon.a

witha2:
	rm -rf build;
	mkdir build;
	make -C common;
	make witha2 -C datanode;
	make -C namenode;
	make -C client;
	$(CC) -I./include/ -g -pthread -lm a2/fs.c a2/ext.c datanode/exta2.c client/dfs_client.c testa2.c -o dfs build/libdfscommon.a

clean:
	make clean -C client;
	make clean -C common;
	make clean -C datanode;
	make clean -C namenode;
	rm -rf build;
	rm -rf test.o;
	rm -f dfs;
