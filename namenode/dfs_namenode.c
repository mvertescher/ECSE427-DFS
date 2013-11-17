#include "namenode/dfs_namenode.h"
#include <assert.h>
#include <unistd.h>

dfs_datanode_t* dnlist[MAX_DATANODE_NUM];
dfs_cm_file_t* file_images[MAX_FILE_COUNT];
int fileCount;
int dncnt;
int safeMode = 1;

struct sockaddr client_address;

int mainLoop(int server_socket)
{
	while (safeMode == 1)
	{
		printf("the namenode is running in safe mode\n");
		sleep(5);
	}

	printf("dfs_namenode.c: mainLoop: Beginning for loop\n");

	int sin_size = sizeof(struct sockaddr);

	for (;;)
	{
		//sockaddr_in client_address;
		//unsigned int client_address_length = sizeof(client_address);
		int client_socket = -1;
		//TODO: accept the connection from the client and assign the return value to client_socket

		printf("dfs_namenode.c: mainLoop: Trying to accept: %i \n", client_socket);


		// int accept (int socket, struct sockaddr *addr, socklen_t *length_ptr) struct sockaddr *)
		client_socket = accept(server_socket, &client_address, &sin_size); // &sizeof??
 		
		printf("dfs_namenode.c: mainLoop: client_socket: %i \n", client_socket);
		assert(client_socket != INVALID_SOCKET);

		dfs_cm_client_req_t request; // clear mem? 
		//TODO: receive requests from client and fill it in request

		printf("dfs_namenode.c: mainLoop: Waiting for recv\n");
		// ssize_t recv (int socket, void *buffer, size_t size, int flags)
		recv(client_socket, &request, sizeof(request), MSG_WAITALL);
		printf("dfs_namenode.c: mainLoop: Got recv\n");

		requests_dispatcher(client_socket, request);
		close(client_socket);
	}
	return 0;
}

static void *heartbeatService()
{
	int socket_handle = create_server_tcp_socket(50030);
	register_datanode(socket_handle);
	close(socket_handle);
	return 0;
}


/**
 * start the service of namenode
 * argc - count of parameters
 * argv - parameters
 */
int start(int argc, char **argv)
{
	assert(argc == 2);
	int i = 0;
	for (i = 0; i < MAX_DATANODE_NUM; i++) dnlist[i] = NULL;
	for (i = 0; i < MAX_FILE_COUNT; i++) file_images[i] = NULL;

	//TODO:create a thread to handle heartbeat service
	//you can implement the related function in dfs_common.c and call it here

	pthread_t *hbThread_id;

	//create_thread(void * (*entry_point)(void*), void *args)
	hbThread_id = create_thread(heartbeatService, (void *)argv[1]); 

	int server_socket = INVALID_SOCKET;
	//TODO: create a socket to listen the client requests and replace the value of server_socket with the socket's fd

	server_socket = create_server_tcp_socket(atoi(argv[1])); 

	assert(server_socket != INVALID_SOCKET);

	safeMode = 0;
	printf("dfs_namenode.c: start(): END, safeMode: %i \n",safeMode);

	return mainLoop(server_socket);
}

int register_datanode(int heartbeat_socket)
{
	for (;;)
	{
		int datanode_socket = -1;
		//TODO: accept connection from DataNodes and assign return value to datanode_socket;
		
		int sin_len = sizeof(struct sockaddr_in);
		struct sockaddr_in datanode_addr;

		// int accept (int socket, struct sockaddr *addr, socklen_t *length_ptr)
		datanode_socket = accept(heartbeat_socket, (struct sockaddr *)&datanode_addr, &sin_len); // &sizeof??



		assert(datanode_socket != INVALID_SOCKET);
		dfs_cm_datanode_status_t datanode_status;
		
		//TODO: receive datanode's status via datanode_socket
		recv(datanode_socket, &datanode_status, sizeof(datanode_status), MSG_WAITALL);


		if (datanode_status.datanode_id < MAX_DATANODE_NUM)
		{
			//TODO: fill dnlist
			int dd_id = datanode_status.datanode_id;
			if (dnlist[dd_id - 1] == NULL) {
				dnlist[dd_id - 1] = (dfs_datanode_t *) malloc(sizeof(dfs_datanode_t));
				dncnt++;
			}

			dnlist[dd_id - 1]->live_marks++;
			strcpy(dnlist[dd_id - 1]->ip, inet_ntoa(datanode_addr.sin_addr));
			dnlist[dd_id -1] -> port = datanode_status.datanode_listen_port;
			//principle: a datanode with id of n should be filled in dnlist[n - 1] (n is always larger than 0)

			//dnlist[n - 1]

			safeMode = 0;
		}


		printf("dsf_namenode.c register_datanode(int heartbeat_socket) : Just registered!");
		close(datanode_socket);
	}
	return 0;
}

int get_file_receivers(int client_socket, dfs_cm_client_req_t request)
{
	printf("Responding to request for block assignment of file '%s'!\n", request.file_name);

	dfs_cm_file_t** end_file_image = file_images + MAX_FILE_COUNT;
	dfs_cm_file_t** file_image = file_images;
	
	// Try to find if there is already an entry for that file
	while (file_image != end_file_image)
	{
		if (*file_image != NULL && strcmp((*file_image)->filename, request.file_name) == 0) break;
		++file_image;
	}

	if (file_image == end_file_image)
	{
		// There is no entry for that file, find an empty location to create one
		file_image = file_images;
		while (file_image != end_file_image)
		{
			if (*file_image == NULL) break;
			++file_image;
		}

		if (file_image == end_file_image) return 1;
		// Create the file entry
		*file_image = (dfs_cm_file_t*)malloc(sizeof(dfs_cm_file_t));
		memset(*file_image, 0, sizeof(*file_image));
		strcpy((*file_image)->filename, request.file_name);
		(*file_image)->file_size = request.file_size;
		(*file_image)->blocknum = 0;
	}
	
	int block_count = (request.file_size + (DFS_BLOCK_SIZE - 1)) / DFS_BLOCK_SIZE;
	
	int first_unassigned_block_index = (*file_image)->blocknum;
	(*file_image)->blocknum = block_count;
	int next_data_node_index = 0;

	//TODO:Assign data blocks to datanodes, round-robin style (see the Documents)
	
	dfs_cm_file_res_t response;
	memset(&response, 0, sizeof(response));

	//dfs_cm_file_t query_result
	response.query_result.blocknum = block_count;

	// Iterate through data node list, lastRoundPoint looks at the last dn, i looks at which block you are assigning 
	// formulate response
	// and update FileImgInMemory

	int i = 0, datanode_index = 0; 
	for (i = 0; i < block_count; i++){
				
		response.query_result.block_list[i].block_id = i;
		file_images[first_unassigned_block_index]->block_list[i].block_id = i;
		 
		response.query_result.block_list[i].loc_port = dnlist[datanode_index]->port;
		file_images[first_unassigned_block_index]->block_list[i].loc_port = dnlist[datanode_index]->port;
		
		memcpy(response.query_result.block_list[i].loc_ip, dnlist[datanode_index]->ip, 16*sizeof(char));	
		memcpy(file_images[first_unassigned_block_index]->block_list[i].loc_ip, dnlist[datanode_index]->ip, 16*sizeof(char));
	
		strcpy(response.query_result.block_list[i].owner_name, file_images[first_unassigned_block_index]->filename);
		datanode_index = (datanode_index + 1) % dncnt;		
	}


	//dfs_cm_file_res_t response;
	//memset(&response, 0, sizeof(response));
	//TODO: fill the response and send it back to the client

	//memcpy(&response,&query_result); 

	if (send(client_socket, &response, sizeof(response), 0) < 0) 
	{
		printf("dfs_namenode.c: get_file_receivers: Client send response failure. \n");
		return 1;
	}

	return 0;
}

int get_file_location(int client_socket, dfs_cm_client_req_t request)
{
	int i = 0;
	for (i = 0; i < MAX_FILE_COUNT; ++i)
	{
		dfs_cm_file_t* file_image = file_images[i];
		if (file_image == NULL) continue;
		if (strcmp(file_image->filename, request.file_name) != 0) continue;
		dfs_cm_file_res_t response;
		//TODO: fill the response and send it back to the client

		return 0;
	}
	//FILE NOT FOUND
	return 1;
}

void get_system_information(int client_socket, dfs_cm_client_req_t request)
{
	printf("dfs_namenode.c: get_system_information: START \n");

	assert(client_socket != INVALID_SOCKET);
	//TODO:fill the response and send back to the client
	dfs_system_status response;
	//response.datanode_num = dnlist[0].dn_id; //???
	response.datanode_num = dncnt; //request.req_type; // NEED TO CHANGE
	//response.datanodes = &dnlist; //???
 
	int send_error = send(client_socket, &response, sizeof(response), 0);
	assert(send_error >= 0);
}

int get_file_update_point(int client_socket, dfs_cm_client_req_t request)
{
	int i = 0;
	for (i = 0; i < MAX_FILE_COUNT; ++i)
	{
		dfs_cm_file_t* file_image = file_images[i];
		if (file_image == NULL) continue;
		if (strcmp(file_image->filename, request.file_name) != 0) continue;
		dfs_cm_file_res_t response;
		//TODO: fill the response and send it back to the client
		// Send back the data block assignments to the client
		memset(&response, 0, sizeof(response));
		//TODO: fill the response and send it back to the client
		return 0;
	}
	//FILE NOT FOUND
	return 1;
}

int requests_dispatcher(int client_socket, dfs_cm_client_req_t request)
{
	//0 - read, 1 - write, 2 - query, 3 - modify
	switch (request.req_type)
	{
		case 0:
			get_file_location(client_socket, request);
			break;
		case 1:
			get_file_receivers(client_socket, request);
			break;
		case 2:
			get_system_information(client_socket, request);
			break;
		case 3:
			get_file_update_point(client_socket, request);
			break;
	}
	return 0;
}

int main(int argc, char **argv)
{
	int i = 0;
	for (; i < MAX_DATANODE_NUM; i++)
		dnlist[i] = NULL;
	return start(argc, argv);
}
