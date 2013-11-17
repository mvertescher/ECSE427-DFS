#include "common/dfs_common.h"
#include "datanode/ext.h"

extern char* working_directory;

void ext_init_local_fs(char* fd) { (void)fd; }

void ext_close_local_fs() {}

int get_block_file_path(char *path, const char *filename, int block_id)
{
	sprintf(path, "%s%s_blk_%d", working_directory, filename, block_id);
	printf("%s\n", path);
	return strlen(path);
}

int ext_read_block(const char *filename, int block_id, void *buf)
{
	char *fullpath = NULL;
	FILE *block_file_ptr = NULL;
	int ret = 0;
	unsigned int block_id_len = block_id / 10;
	if (0 == block_id_len) block_id_len = 1;
	else block_id_len = 2;
	fullpath = (char *) malloc(sizeof(char) * (strlen(working_directory) + strlen(filename) + block_id_len + 7));
	get_block_file_path(fullpath, filename, block_id);
	block_file_ptr = fopen(fullpath, "rb");
	ret = fread(buf, DFS_BLOCK_SIZE, 1, block_file_ptr);
	fclose(block_file_ptr);
	free(fullpath);
	return ret;
}

int ext_write_block(const char *filename, int block_id, void *buf)
{
	char *fullpath = NULL;
	FILE *block_file_ptr = NULL;
	int ret = 0;
	unsigned int block_id_len = block_id / 10;
	if (0 == block_id_len) block_id_len = 1;
	else block_id_len = 2;
	fullpath = (char *) malloc(sizeof(char) * (strlen(working_directory) + strlen(filename) + block_id_len + 7));
	get_block_file_path(fullpath, filename, block_id);
	block_file_ptr = fopen(fullpath, "wb");
	ret = fwrite(buf, DFS_BLOCK_SIZE, 1, block_file_ptr);
	printf("write successfully on %s\n", fullpath);
	fclose(block_file_ptr);
	free(fullpath);
	return ret;
}
