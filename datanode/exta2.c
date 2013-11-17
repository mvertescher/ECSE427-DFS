#include "common/dfs_common.h"
#include "datanode/ext.h"
#include "../a2/ext.h"
#include "../a2/fs.h"

int get_block_file_path(char *path, const char *filename, int block_id)
{
	sprintf(path, "%s_blk_%d", filename, block_id);
	return strlen(path);
}

int ext_read_block(const char *filename, int block_id, void *buf)
{
	char *blk_path = (char *)malloc(sizeof(char) * strlen(filename) + 7);
	int fd = 0;
	get_block_file_path(blk_path, filename, block_id);
	fd = sfs_open("/", blk_path);
	sfs_read(fd, buf, DFS_BLOCK_SIZE);
	sfs_close(fd);
	return 0;
}

int ext_write_block(const char *filename, int block_id, void *buf)
{
	char *blk_path = (char *)malloc(sizeof(char) * strlen(filename) + 7);
	int fd = 0;
	get_block_file_path(blk_path, filename, block_id);
	int dirbid = sfs_mkdir("/");
	fd = sfs_open("/", blk_path);
	int a = sfs_write(fd, buf, DFS_BLOCK_SIZE);
	printf("successfully write %s to file %d\n", blk_path, fd);
	sfs_print_info();
	sfs_close(fd);
	return 0;
}

void ext_init_local_fs(char *wd)
{
	printf("initializing local file system...\n");
	char *local_fs_path = (char *) malloc(sizeof(char) * strlen(wd) + strlen("local_fs") + 1);
	sprintf(local_fs_path, "%s%s", wd, "local_fs");
	sfs_init_storage(local_fs_path);
	sfs_mkfs();
	free(local_fs_path);
}

void ext_close_local_fs()
{
	sfs_close_storage();
}
