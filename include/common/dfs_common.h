#ifndef DFS_COMMON_H
#define DFS_COMMON_H

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>

#define INVALID_SOCKET (-1)

#define MAX_FILE_COUNT 1000
#define MAX_DATANODE_NUM 10
#define DFS_BLOCK_SIZE 1024 
#define HEARTBEAT_INTERVAL 3
#define BLOCK_SIZE 512

#define MAX_FILE_BLK_COUNT 100

typedef struct sockaddr sockaddr;
typedef struct sockaddr_in sockaddr_in;

//share by namenode and datanode
typedef struct _dfs_dn_status_
{
	int datanode_id;
	int datanode_listen_port;
}dfs_cm_datanode_status_t;


typedef struct _datanode_descriptor_
{
	int dn_id;
	char ip[16];
	int port;
	unsigned int live_marks;
}dfs_datanode_t;

typedef struct _block_descriptor_
{
	char owner_name[256];
	int dn_id;
	int block_id;
	char loc_ip[16];
	int loc_port;
	char content[DFS_BLOCK_SIZE];
}dfs_cm_block_t;

typedef struct _file_descriptor_
{
	char filename[256];
	dfs_cm_block_t block_list[MAX_FILE_BLK_COUNT];
	int file_size;
	int blocknum;
}dfs_cm_file_t;

typedef struct _system_descriptor_
{
	int datanode_num;
	dfs_datanode_t datanodes[MAX_FILE_BLK_COUNT];		
}dfs_system_status;

//a wrapper of a dfs_cm_file_t
typedef struct _file_query_response_
{
	dfs_cm_file_t query_result;
}dfs_cm_file_res_t;

typedef struct _dfs_client_request_
{
	char file_name[256];
	int file_size;
	int req_type;//0-read, 1-write, 2-query datanodes, 3 - modify file;
}dfs_cm_client_req_t;

//shared by datanode and client
typedef struct _dfs_client_datanode_req_
{
	int op_type;//0-read, 1-create
	dfs_cm_block_t block;
} dfs_cli_dn_req_t;

inline pthread_t * create_thread(void * (*entry_point)(void *), void *args);
int create_client_tcp_socket(char* address, int port);
int create_server_tcp_socket(int port);
void send_data(int socket, void* data, int size);
void receive_data(int socket, void* data, int size);
#endif
