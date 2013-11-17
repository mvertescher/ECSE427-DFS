#include "client/dfs_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

//test connection between datanode and namenode
int test_case_0(char **argv, int op_type)
{
	printf("test.c: test_case_0: START \n");  

	dfs_system_status *sys_stat = NULL;
	if ((sys_stat = send_sysinfo_request(argv)) == NULL) return 1;
	if (sys_stat->datanode_num == 2) 
	{
		free(sys_stat);
		return 0;
	}
	return 1;
}

//write 
int test_case_1(char **argv, int op_type)
{
	if (send_file_request(argv, "local_file", 1) == -1) 
	{
		return 1;
	}
	int ret = 0;
	char * str_arr[1];
	char *blk_0 = "d1/local_file_blk_0";
	FILE *local_fp = fopen("local_file", "rb");
	char *local_buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	char *buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	sleep(5);
	str_arr[0] = blk_0;
	FILE *fp = fopen(str_arr[0], "rb");
	if (fp == NULL) return 1;
	memset(buf, 0, DFS_BLOCK_SIZE);
	memset(local_buf, 0, DFS_BLOCK_SIZE);
	fread(buf, DFS_BLOCK_SIZE, 1, fp);
	fread(local_buf, DFS_BLOCK_SIZE, 1, local_fp);
	if (memcmp(local_buf, buf, DFS_BLOCK_SIZE) != 0) 
	{
		ret = 1;
	}
	fclose(fp);
	free(buf);
	free(local_buf);
	fclose(local_fp);
	return ret;
}

int test_case_2(char **argv, int op_type)
{
	system("rm -f local_file");
	if (send_file_request(argv, "local_file", 0) == -1)
	{
		return 1; 
	}
	int ret = 0;
	char *buf = (char *) malloc(sizeof(char) * 1024);
	char *buf_1 = (char *) malloc(sizeof(char) * 1024);
	FILE *fp = fopen("local_file", "rb");
	sleep(5);
	fread(buf, 1024, 1, fp);
	fclose(fp);
	fp = fopen("local_file_1", "rb");
	fread(buf_1, 1024, 1, fp);
	fclose(fp);
	if (memcmp(buf, buf_1, 1024) != 0)
		ret = 1;
	free(buf);
	free(buf_1);
	return ret;
}

int test_case_3(char **argv, int op_type)
{
	if (send_file_request(argv, "local_file_medium", 1) == -1) 
	{
		return 1;
	}
	sleep(5);
	int ret = 0;
	int i = 0;
	char * str_arr[4];
	char *blk_0 = "d1/local_file_medium_blk_0";
	char *blk_1 = "d2/local_file_medium_blk_1";
	char *blk_2 = "d1/local_file_medium_blk_2";
	char *blk_3 = "d2/local_file_medium_blk_3";

	FILE *local_fp = fopen("local_file_medium", "rb");
	char *local_buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	char *buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	str_arr[0] = blk_0;
	str_arr[1] = blk_1;
	str_arr[2] = blk_2;
	str_arr[3] = blk_3;
	for (; i < 4; i++)
	{
		FILE *fp = fopen(str_arr[i], "rb");
		if (fp == NULL)
		{
			ret = 1;
			break;
		}
		memset(buf, 0, DFS_BLOCK_SIZE);
		memset(local_buf, 0, DFS_BLOCK_SIZE);
		fread(buf, DFS_BLOCK_SIZE, 1, fp);
		fseek(local_fp, i * DFS_BLOCK_SIZE, SEEK_SET);
		fread(local_buf, DFS_BLOCK_SIZE, 1, local_fp);
		if (memcmp(local_buf, buf, DFS_BLOCK_SIZE) != 0) 
		{
			printf("failed on %d\n", i);
			fclose(fp);
			ret = 1;
			break;
		}
		fclose(fp);
	}
	free(buf);
	free(local_buf);
	fclose(local_fp);
	return ret;
}

int test_case_4(char **argv, int op_type)
{
	system("rm -f local_file_medium");
	if (send_file_request(argv, "local_file_medium", 0) == -1)
	{
		return 1;
	}
	sleep(5);
	int ret = 0;
	FILE *fp = fopen("local_file_medium", "rb");
	char *buf = (char *) malloc(sizeof(char) * 4096);
	char *buf_1 = (char *) malloc(sizeof(char) * 4096);

	fread(buf, 4096, 1, fp);
	fclose(fp);
	fp = fopen("local_file_medium_1", "rb");
	fread(buf_1, 4096, 1, fp);
	fclose(fp);
	if (memcmp(buf, buf_1, 4096) != 0)
		ret = 1;
	free(buf);
	free(buf_1);
	return ret;
}

int test_case_5(char **argv, int op_type)
{
	if (send_file_request(argv, "local_file_large", 1) == -1)
	{
		return 1;
	}
	sleep(5);
	int ret = 0;
	int i = 0;
	char * str_arr[8];
	char *blk_0 = "d1/local_file_large_blk_0";
	char *blk_1 = "d2/local_file_large_blk_1";
	char *blk_2 = "d1/local_file_large_blk_2";
	char *blk_3 = "d2/local_file_large_blk_3";
	char *blk_4 = "d1/local_file_large_blk_4";
	char *blk_5 = "d2/local_file_large_blk_5";
	char *blk_6 = "d1/local_file_large_blk_6";
	char *blk_7 = "d2/local_file_large_blk_7";

	FILE *local_fp = fopen("local_file_large", "rb");
	char *local_buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	char *buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	str_arr[0] = blk_0;
	str_arr[1] = blk_1;
	str_arr[2] = blk_2;
	str_arr[3] = blk_3;
	str_arr[4] = blk_4;
	str_arr[5] = blk_5;
	str_arr[6] = blk_6;
	str_arr[7] = blk_7;

	for (; i < 8; i++)
	{
		FILE *fp = fopen(str_arr[i], "rb");
		if (fp == NULL)
		{
			ret = 1;
			break;
		}
		memset(buf, 0, DFS_BLOCK_SIZE);
		memset(local_buf, 0, DFS_BLOCK_SIZE);
		fread(buf, DFS_BLOCK_SIZE, 1, fp);
		fseek(local_fp, i * DFS_BLOCK_SIZE, SEEK_SET);
		fread(local_buf, DFS_BLOCK_SIZE, 1, local_fp);
		if (memcmp(local_buf, buf, DFS_BLOCK_SIZE) != 0) 
		{
			printf("failed on %d\n", i);
			fclose(fp);
			ret = 1;
			break;
		}
		fclose(fp);
	}
	free(buf);
	free(local_buf);
	fclose(local_fp);
	return ret;
}

int test_case_6(char **argv, int op_type)
{
	system("rm -f local_file_large");
	if (send_file_request(argv, "local_file_large", 0) == -1)
	{
		return 1;
	}
	sleep(5);
	int ret = 0;
	FILE *fp = fopen("local_file_large", "rb");
	char *buf = (char *) malloc(sizeof(char) * 8192);
	char *buf_1 = (char *) malloc(sizeof(char) * 8192);

	fread(buf, 8192, 1, fp);
	fclose(fp);
	fp = fopen("local_file_large_1", "rb");
	fread(buf_1, 8192, 1, fp);
	fclose(fp);
	if (memcmp(buf, buf_1, 8192) != 0)
		ret = 1;
	free(buf);
	free(buf_1);
	return ret;
}

int test_case_7(char **argv) 
{
	char **args = (char **) malloc(sizeof(char *) * 3);
	args[0] = argv[0];
	args[1] = argv[1];
	args[2] = "local_file";
	int r2 = test_case_2(argv, 1024);
	args[2] = "local_file_medium";
	int r6 = test_case_4(argv, 4096);
	args[2] = "local_file_large";
	int r8	= test_case_6(argv, 8192);
	return r2 || r6 || r8;
}

int test_case_8(char **argv, int op_type)
{
	int ret = 0;
	int i = 0;
	char * str_arr[2];
	char *blk_0 = "d1/local_file_blk_0";
	char *blk_1 = "d2/local_file_blk_1";
	FILE *local_fp = fopen("local_file", "rb");
	char *local_buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	char *buf = (char *) malloc(sizeof(char) * DFS_BLOCK_SIZE);
	if (modify_file(argv[1], atoi(argv[2]), "local_file", 2048, 1024, 2047) == -1) 
	{
		fclose(local_fp);
		return 1; 
	}
	sleep(5);
	str_arr[0] = blk_0;
	str_arr[1] = blk_1;
	for (; i < 2; i++) {
		FILE *fp = fopen(str_arr[i], "rb");
		if (fp == NULL) return 1;
		memset(buf, 0, DFS_BLOCK_SIZE);
		memset(local_buf, 0, DFS_BLOCK_SIZE);
		fread(buf, DFS_BLOCK_SIZE, 1, fp);
		fseek(local_fp, i * DFS_BLOCK_SIZE, SEEK_SET);
		fread(local_buf, DFS_BLOCK_SIZE, 1, local_fp);
		if (memcmp(local_buf, buf, DFS_BLOCK_SIZE) != 0) 
		{
			printf("failed on %d\n", i);
			ret = 1;
		}
		fclose(fp);
	}
	free(buf);
	free(local_buf);
	fclose(local_fp);
	return ret;
}

void append_data(char *out_file, int append_size)
{
	int i = 0;
	FILE *fp = fopen(out_file, "a+");
	srand((unsigned)time(0));
	for (; i < append_size; i++)
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

void generate_data(char *out_file, int file_size)
{
	int i = 0;
	FILE *fp = fopen(out_file, "wb");
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
	
	printf("TEST CASE 0:%s\n", result[test_case_0(argv, 2)]);
	//generate data
	//can contact to single datanode
	generate_data("local_file", 1024);
	printf("TEST CASE 1:%s\n", result[test_case_1(argv, 1)]);
	printf("TEST CASE 2:%s\n", result[test_case_2(argv, 0)]);
	//can contact to two datanodes	
	generate_data("local_file_medium", 4096);
	printf("TEST CASE 3:%s\n", result[test_case_3(argv, 1)]);
	printf("TEST CASE 4:%s\n", result[test_case_4(argv, 0)]);
	//can handle chunk pieces
	generate_data("local_file_large", 8192);
	printf("TEST CASE 5:%s\n", result[test_case_5(argv, 1)]);
	printf("TEST CASE 6:%s\n", result[test_case_6(argv, 0)]);
	//check every file is stored correctly
	printf("TEST CASE 7:%s\n", result[test_case_7(argv)]);
	//modify the file
	append_data("local_file", 1024);
	printf("TEST CASE 8:%s\n", result[test_case_8(argv, 3)]);
	return 0;
}
