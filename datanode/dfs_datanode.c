#include "datanode/dfs_datanode.h"
#include <assert.h>
#include <errno.h>
#include <unistd.h>

int lastHeartbeatStamp = 0;//last time the datanode contacted the namenode

int datanode_id = 0;
int datanode_listen_port = 0;
char *working_directory = NULL;

// main service loop of datanode
int mainLoop()
{
	//we don't consider concurrent operations in this assignment
	int server_socket = -1;
	
	//TODO: create a server socket and listen on it, you can implement dfs_common.c and call it here
	server_socket = create_server_tcp_socket(datanode_listen_port); 

	assert (server_socket != INVALID_SOCKET);

	// Listen to requests from the clients
	for (;;)
	{
		sockaddr_in client_address;
		int client_socket = -1;
		//TODO: accept the client request

		// int accept (int socket, struct sockaddr *addr, socklen_t *length_ptr)
		client_socket = accept(server_socket, (struct sockaddr *)&client_address, sizeof(client_address)); // &sizeof??

		assert(client_socket != INVALID_SOCKET);
		dfs_cli_dn_req_t request;
		//TODO: receive data from client_socket, and fill it to request

		// ssize_t recv (int socket, void *buffer, size_t size, int flags)
		recv(client_socket, &request, sizeof(request), MSG_WAITALL);


		requests_dispatcher(client_socket, request);
		close(client_socket);
	}
	close(server_socket);
	return 0;
}

static void *heartbeat()
{
	dfs_cm_datanode_status_t datanode_status;
	datanode_status.datanode_id = datanode_id;
	datanode_status.datanode_listen_port = datanode_listen_port;

	struct sockaddr_in namenode_addr;
	
	memset((void *)&nn_addr, 0, sizeof(nn_addr));
		
	//TODO: set nn_addr
	nn_addr.sin_family = AF_INET;
	nn_addr.sin_addr.s_addr = inet_addr(nn_ip);
	nn_addr.sin_port = htons(hb_port);

	for (;;)
	{
		int heartbeat_socket = -1;
		//TODO: create a socket to the namenode, assign file descriptor id to heartbeat_socket
		heartbeat_socket = create_tcp_socket();

		assert(heartbeat_socket != INVALID_SOCKET);
		//send datanode_status to namenode
		
		if (connect(heartbeat_socket, (struct sockaddr *) &nn_addr, sizeof(nn_addr) ) == -1) printf("dfs_datanode.c: heartbeat: Connect error. \n");
		if (send(heartbeat_socket, &ds, sizeof(ds), 0 ) == -1) printf("dfs_datanode.c: heartbeat: Send error. \n");

		close(heartbeat_socket);
		sleep(HEARTBEAT_INTERVAL);
	}
}

/**
 * start the service of data node
 * argc - count of parameters
 * argv - parameters
 */
int start(int argc, char **argv)
{
	assert(argc == 5);
	char namenode_ip[32] = { 0 };
	strcpy(namenode_ip, argv[2]);

	datanode_id = atoi(argv[3]);
	datanode_listen_port = atoi(argv[1]);
	working_directory = (char *)malloc(sizeof(char) * strlen(argv[4]) + 1);
	strcpy(working_directory, argv[4]);
	//start one thread to report to the namenode periodically
	//TODO: start a thread to report heartbeat

	return mainLoop();
}

int read_block(int client_socket, const dfs_cli_dn_req_t *request)
{
	assert(client_socket != INVALID_SOCKET);
	assert(request != NULL);
	char buffer[DFS_BLOCK_SIZE];
	ext_read_block(request->block.owner_name, request->block.block_id, (void *)buffer);
	//TODO:response the client with the data
	return 0;
}

int create_block(const dfs_cli_dn_req_t* request)
{
	ext_write_block(request->block.owner_name, request->block.block_id, (void *)request->block.content);
	return 0;
}

void requests_dispatcher(int client_socket, dfs_cli_dn_req_t request)
{
	switch (request.op_type)
	{
		case 0:
			read_block(client_socket, &request);
			break;
		case 1:
			create_block(&request);
			break;
	}
}

int main(int argc, char **argv)
{
	if (argc != 5)
	{
		printf("usage:datanode local_port namenode_ip id working_directory\n");
		return 1;
	}
	ext_init_local_fs(argv[4]);
	start(argc, argv);
	ext_close_local_fs();
	return 0;
}
