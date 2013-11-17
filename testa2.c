#include "client/dfs_client.h"
#include <unistd.h>
#include "a2/fs.h"


int test_case_1(char **argv, int op_type)
{
	if (send_file_request(argv, "local_file", 1) == -1) 
	{
		return 1;
	}
	sleep(5);
	int ret = 0;
	int fd;
	FILE *local_fp = fopen("local_file", "rb");
	char *buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	char *buf_local = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	
	sfs_reloadfs("d1/local_fs");
	fd = sfs_open("/", "local_file_blk_0");
	sfs_read(fd, buf, DFS_BLOCK_SIZE);
	sfs_close(fd);
	fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
	if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	if (ret == 0)
	{
		fseek(local_fp, 2 * DFS_BLOCK_SIZE, SEEK_SET); 
		fd = sfs_open("/", "local_file_blk_2");
		sfs_read(fd, buf, DFS_BLOCK_SIZE); 
		sfs_close(fd);
		fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
		if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	}
	if (ret == 0)
	{
		fseek(local_fp, 4 * DFS_BLOCK_SIZE, SEEK_SET);
		fd = sfs_open("/", "local_file_blk_4");
		sfs_read(fd, buf, DFS_BLOCK_SIZE); 
		sfs_close(fd);
		fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
		if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	}
	if (ret == 0)
	{
		fseek(local_fp, 6 * DFS_BLOCK_SIZE, SEEK_SET);
		fd = sfs_open("/", "local_file_blk_6");
		sfs_read(fd, buf, DFS_BLOCK_SIZE); 
		sfs_close(fd);
		fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
		if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	}
	sfs_close_storage();
	
	sfs_reloadfs("d2/local_fs");
	fd = sfs_open("/", "local_file_blk_1");
	sfs_read(fd, buf, DFS_BLOCK_SIZE);
	sfs_close(fd);
	fseek(local_fp, 1 * DFS_BLOCK_SIZE, SEEK_SET);
	fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
	if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	if (ret == 0)
	{
		fseek(local_fp, 3 * DFS_BLOCK_SIZE, SEEK_SET);
		fd = sfs_open("/", "local_file_blk_3");
		sfs_read(fd, buf, DFS_BLOCK_SIZE); 
		sfs_close(fd);
		fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
		if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	}
	if (ret == 0)
	{
		fseek(local_fp, 5 * DFS_BLOCK_SIZE, SEEK_SET);
		fd = sfs_open("/", "local_file_blk_5");
		sfs_read(fd, buf, DFS_BLOCK_SIZE); 
		sfs_close(fd);
		fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
		if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	}
	if (ret == 0)
	{
		fseek(local_fp, 7 * DFS_BLOCK_SIZE, SEEK_SET);
		fd = sfs_open("/", "local_file_blk_7");
		sfs_read(fd, buf, DFS_BLOCK_SIZE); 
		sfs_close(fd);
		fread(buf_local, 1, DFS_BLOCK_SIZE, local_fp);
		if (memcmp(buf_local, buf, DFS_BLOCK_SIZE) != 0) ret = 1;
	}
	sfs_close_storage();
	fclose(local_fp);
	free(buf);
	free(buf_local);
	return ret;
}

int test_case_2(char **argv, int op_type)
{
	system("rm local_file");
	if (send_file_request(argv, "local_file", 0) == -1) {
		return 1;
	}
	sleep(5);
	int ret = 0;
	FILE *fp = fopen("local_file", "rb");
	char *buf = (char *) malloc(sizeof(char) * 8192);
	char *buf_1 = (char *) malloc(sizeof(char) * 8192);
	fread(buf, 8192, 1, fp);
	fclose(fp);
	fp = fopen("local_file_1", "rb");
	fread(buf_1, 8192, 1, fp);
	fclose(fp);
	if (memcmp(buf, buf_1, 8192) != 0)
		ret = 1;
	free(buf);
	free(buf_1);
	return ret;
}


void generate_data(char *out_file, int file_size)
{
	int i = 0;
	FILE *fp = fopen(out_file, "w+b");
	srand((unsigned)time(0));
	for (; i < file_size; i++)
	{
		char c = '0' + rand() % 10;
		fputc(c, fp);
	}
	fclose(fp);
	char *cmd = (char *) malloc(sizeof(char) * (strlen(out_file) * 2 + 6));
	sprintf(cmd, "cp %s %s_1", out_file, out_file);
	system(cmd);
	free(cmd);
}


int main(int argc, char **argv)
{
	assert(argc == 3);
	char *result[2];
	result[0] = "PASS";
	result[1] = "FAILED";
	//generate data
	//can contact to single datanode
	generate_data("local_file", 8192);
	printf("TEST CASE 1:%s\n", result[test_case_1(argv, 1)]);
	printf("TEST CASE 2:%s\n", result[test_case_2(argv, 0)]);
	return 0;
}
