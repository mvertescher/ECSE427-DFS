#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//TODO: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 
	int client_socket = -1;

	client_socket = create_client_tcp_socket(address, port); //int create_client_tcp_socket(char* address, int port);

	if (client_socket == -1)
		printf("dfs_common.c: connect_to_nn: ERROR: client_socket = -1 \n"); 

	return client_socket;
}

int modify_file(char *ip, int port, const char* filename, int file_size, int start_addr, int end_addr)
{
	printf("dfs_client.c: modify_file: Start. file = %s \n",filename);
	int namenode_socket = connect_to_nn(ip, port);
	if (namenode_socket == INVALID_SOCKET) return -1;
	FILE* file = fopen(filename, "rb");
	assert(file != NULL);

	//TODO:fill the request and send
	dfs_cm_client_req_t request;
	strcpy(request.file_name, filename);
	request.file_size = file_size;

	//offset -- This is the number of bytes to offset from whence.
	//whence -- This is the position from where offset is added.
 	//fseek(file, 0, SEEK_END); 
	//request.file_size = ftell(file);
	//fseek(file, 0, SEEK_SET);

	request.req_type = 3; //modify file
	
	//TODO:fill the fields in request and send 
 	if(send(namenode_socket, &request, sizeof(request), 0) == -1 ) // Send request
 		printf("dfs_client.c: modify_file: Namenode request send failure. \n"); 

	//TODO: receive the response
	dfs_cm_file_res_t response;
	while(recv(namenode_socket, &response, sizeof(response), MSG_WAITALL) == -1);
	//TODO: send the updated block to the proper datanode

	int total_chunks = ((file_size - 1)/DFS_BLOCK_SIZE + 1);
	int cur_chunks = response.query_result.blocknum;
	
	// Check if we need to expand the file: 
	printf("dfs_client.c: modify_file: response.query_result.blocknum: %i need %i \n",cur_chunks,total_chunks); 
	printf("dfs_client.c: modify_file: response.query_result.filename: %s \n",response.query_result.filename); 
	printf("dfs_client.c: modify_file: response.query_result.file_size %i \n",response.query_result.file_size); 



	// query_result: char filename[256]; dfs_cm_block_t block_list[MAX_FILE_BLK_COUNT]; int file_size; int blocknum;
	
	int first_block_index = start_addr / DFS_BLOCK_SIZE;
	int last_block_index = end_addr / DFS_BLOCK_SIZE;

	int first_block_start = start_addr % DFS_BLOCK_SIZE;
	int last_block_end = end_addr % DFS_BLOCK_SIZE;
	
	//response.query_result

	char *buffer = (char *)calloc(DFS_BLOCK_SIZE, sizeof(char));
	//char *buf_pnt = buffer;
	int i, datanode_socket = 0;
	//struct sockaddr_in block_dest;
	//dfs_cli_dn_req_t client_datanode_request;
	//dfs_cli_dn_req_t pull_block_request;


	// Seek to beginning of read
	fseek(file, start_addr, SEEK_SET);

	printf("dfs_client.c: modify_file: Beginning to send blocks to datanode one by one: first_block_index: %i last_block_index: %i \n",first_block_index,last_block_index);
	//TODO: Send blocks to datanodes one by one
	for(i = first_block_index; i <= last_block_index; i++) {
		
		// Connect to the datanode and return the socket 
		datanode_socket = create_client_tcp_socket(response.query_result.block_list[i].loc_ip, response.query_result.block_list[i].loc_port); 
		memset(buffer, 0, DFS_BLOCK_SIZE); // Clear the buffer
		fread(buffer, sizeof(char), DFS_BLOCK_SIZE, file); // Read the current block from file
		// Send the block to the datanode
		send_block_to_datanode(datanode_socket, buffer, DFS_BLOCK_SIZE, response.query_result.block_list[i].owner_name, i);

	}
	
	free(buffer);
	fclose(file);
	return 0;
}

int push_file(int namenode_socket, const char* local_path)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(local_path != NULL);
	FILE* file = fopen(local_path, "rb");
	assert(file != NULL);

	printf("dfs_client.c: push_file: Pushing %s to datanodes. \n",local_path);

	// Create the push request
	dfs_cm_client_req_t request; //char file_name[256]; int file_size; int req_type;//0-read, 1-write, 2-query datanodes, 3 - modify file;
	
	//Copy the name
	strcpy(request.file_name,local_path);
 	
 	// Get file size 
 	fseek(file, 0, SEEK_END); // Seek to the end of the file
	request.file_size = ftell(file); // ftell: returns the current offset in the file or -1 on error 
	fseek(file, 0, SEEK_SET); // Seek to the beginning of the file 

 	request.req_type = 1; // 1-write 


	//TODO:fill the fields in request and send 
 	if(send(namenode_socket, &request, sizeof(request), 0) == -1 ) // Send request
 		printf("dfs_client.c: push_file: Namenode request send failure. \n"); 


	//TODO:Receive the response
	dfs_cm_file_res_t response; // New dfs_cm_file_res_t response struct on the stack

	while(recv(namenode_socket, &response, sizeof(response), MSG_WAITALL) == -1); // Fill the response from namenode

	char *buffer = (char *)calloc(DFS_BLOCK_SIZE, sizeof(char));
	int i, datanode_socket = 0;

	//TODO: Send blocks to datanodes one by one
	for (i = 0; i < response.query_result.blocknum; i++) 
	{
		// Connect to the datanode and return the socket 
		datanode_socket = create_client_tcp_socket(response.query_result.block_list[i].loc_ip, response.query_result.block_list[i].loc_port); 
		memset(buffer, 0, DFS_BLOCK_SIZE); // Clear the buffer
		fread(buffer, sizeof(char), DFS_BLOCK_SIZE, file); // Read the current block from file
		printf("dfs_client.c: push_file: Pushing block_id = %i of %s to datanode at %s:%i. \n",
			i,local_path,response.query_result.block_list[i].loc_ip, response.query_result.block_list[i].loc_port);
		// Send the block to the datanode
		send_block_to_datanode(datanode_socket, buffer, DFS_BLOCK_SIZE, response.query_result.block_list[i].owner_name, i); 
	}
	
	free(buffer);
	fclose(file);
	return 0;
}

int pull_file(int namenode_socket, const char *filename)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	printf("dfs_client.c: pull_file: Pulling %s from datanodes. \n",filename); 

	//TODO: fill the request, and send
	dfs_cm_client_req_t request; // New namenode request 
	memset(&request, 0, sizeof(dfs_cm_client_req_t)); // Set the request to zero 
	request.req_type = 0; // Signal read from name node
	strcpy(request.file_name, filename); // Copy in the filename

	//send request to namenode through namenode_socket
	send(namenode_socket, &request, sizeof(request), 0);
	
	//TODO: Get the response	
	dfs_cm_file_res_t response;
	
	assert(recv(namenode_socket, &response, sizeof(dfs_cm_file_res_t), MSG_WAITALL) != -1);
	assert(response.query_result.blocknum > 0);  

	int i = 0;
	int datanode_socket = 0;
	char *buffer = (char *)calloc(DFS_BLOCK_SIZE, sizeof(char));

	//TODO: Receive blocks from datanodes one by one
	for (i = 0; i < response.query_result.blocknum; i++) 
	{
		// Connect to the datanode and return the socket 
		datanode_socket = create_client_tcp_socket(response.query_result.block_list[i].loc_ip, response.query_result.block_list[i].loc_port); 

		memset(buffer, 0, DFS_BLOCK_SIZE); // Clear the buffer
		
		// Get the block from the datanode, the block fills the buffer 
		fetch_block_from_datanode(datanode_socket, buffer, DFS_BLOCK_SIZE, filename, i);
		
		// Copy the buffer into the response buffer
		memcpy(response.query_result.block_list[i].content,buffer,DFS_BLOCK_SIZE);
	}
	
	//TODO: reassemble the received blocks into the complete file
	FILE *file = fopen(filename, "wb");
	for (i = 0; i < response.query_result.blocknum; i++) 
		fwrite(response.query_result.block_list[i].content, DFS_BLOCK_SIZE, 1, file);

	free(buffer);
	fclose(file);
	return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//TODO fill the result and send 
	dfs_cm_client_req_t request;
	memset((void *)&request, 0, sizeof(request)); // Set struct memory to 0
	request.req_type = 2; // Datanode query 

	printf("dfs_client.c: get_system_info: Requesting system information from namenode \n");

	send(namenode_socket, &request, sizeof(request), 0); // Send request 

	//TODO: get the response
	dfs_system_status *response = (dfs_system_status *) malloc(sizeof(dfs_system_status));  
	memset((void *)response, 0, sizeof(sizeof(dfs_system_status)));

	if (recv(namenode_socket, (void *) response, sizeof(dfs_system_status), MSG_WAITALL) == -1)
		printf("dfs_client.c: get_system_info: ERROR: recv\n");
	
	printf("dfs_client.c: get_system_info: Received response from namenode. response.datanode_num: %i \n", (*response).datanode_num);
	
	return response;		
}

int send_file_request(char **argv, char *filename, int op_type)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return -1;
	}

	int result = 1;
	switch (op_type)
	{
		case 0:
			result = pull_file(namenode_socket, filename);
			break;
		case 1:
			result = push_file(namenode_socket, filename);
			break;
	}
	close(namenode_socket);
	return result;
}

dfs_system_status *send_sysinfo_request(char **argv)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return NULL;
	}
	dfs_system_status* ret =  get_system_info(namenode_socket);
	close(namenode_socket);

	return ret;
}
