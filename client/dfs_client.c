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

	printf("dfs_common.c: connect_to_nn: END \n"); 
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
 	fseek(file, 0, SEEK_END); 
	request.file_size = ftell(file);
	fseek(file, 0, SEEK_SET);

	request.req_type = 3; //modify file
	
	//TODO:fill the fields in request and send 
 	if(send(namenode_socket, &request, sizeof(request), 0) == -1 ) // Send request
 		printf("dfs_client.c: modify_file: Namenode request send failure. \n"); 

	//TODO: receive the response
	dfs_cm_file_res_t response;
	while(recv(namenode_socket, &response, sizeof(response), MSG_WAITALL) == -1);
	//TODO: send the updated block to the proper datanode

	// query_result: char filename[256]; dfs_cm_block_t block_list[MAX_FILE_BLK_COUNT]; int file_size; int blocknum;
	
	int first_block_index = start_addr / DFS_BLOCK_SIZE;
	int last_block_index = end_addr / DFS_BLOCK_SIZE;

	int first_block_start = start_addr % DFS_BLOCK_SIZE;
	int last_block_end = end_addr % DFS_BLOCK_SIZE;
	
	//response.query_result

	char *buffer = (char *)calloc(DFS_BLOCK_SIZE, sizeof(char));
	char *buf_pnt = buffer;
	int i, datanode_socket = 0;
	struct sockaddr_in block_dest;
	dfs_cli_dn_req_t client_datanode_request;
	dfs_cli_dn_req_t pull_block_request;


	// Seek to beginning of read
	fseek(file, start_addr, SEEK_SET);

	printf("dfs_client.c: modify_file: Beginning to send blocks to datanode one by one: first_block_index: %i last_block_index: %i \n",first_block_index,last_block_index);
	//TODO: Send blocks to datanodes one by one
	for(i = first_block_index; i <= last_block_index; i++) {
		printf("dfs_client.c: modify_file: Beginning of for loop iteration (each block in file) \n");

		// 1 - Create a socket for the datanode 
		datanode_socket = create_tcp_socket(); 

		// 2 - Get the datanode address from the response send by namenode
		memset(&block_dest, 0, sizeof(block_dest));
		block_dest.sin_family = AF_INET;
		block_dest.sin_addr.s_addr = inet_addr(response.query_result.block_list[i].loc_ip);
		block_dest.sin_port = htons(response.query_result.block_list[i].loc_port);

		printf("dfs_client.c: modify_file: Attempting to connect to datanode: %s:%i \n", response.query_result.block_list[i].loc_ip,response.query_result.block_list[i].loc_port);

		// 3 - Connect to the datanode
		if(datanode_socket == -1 || connect(datanode_socket, (struct sockaddr *) &block_dest, sizeof (struct sockaddr)) < 0 ) {
			if(datanode_socket != -1) {
				close (datanode_socket);
			}
			printf("dfs_client.c: modify_file: Datanode connect failure. \n");
			fclose(file);
			free(buffer);
			return -1;
		}

		// 4 - Fill pull request struct 
		pull_block_request.op_type = 0; // READ
		strcpy(pull_block_request.block.owner_name, filename);
		pull_block_request.block.block_id = i;
		printf("dfs_client.c : modify_file() : pull_block_request.block.owner_name: %s \n",pull_block_request.block.owner_name);

		// 5 - Send pull request 
		printf("dfs_client.c : modify_file() : about to send pull_block_request over datanode_socket\n");
		send(datanode_socket, &pull_block_request, sizeof(pull_block_request), 0);

		// Clear buffer
		memset(buffer, 0, DFS_BLOCK_SIZE);
		
		// 6 - Receive response and put into buffer
		recv(datanode_socket, buffer, DFS_BLOCK_SIZE, MSG_WAITALL);

		// Only one iteration - write to part of buffer
		if (first_block_index == last_block_index) {
			buf_pnt = buf_pnt + first_block_index;
			fread(buf_pnt, sizeof(char), (last_block_index - first_block_index), file);
		}

		// Write from first_block_start to DFS_BLOCK_SIZE
		else if (i == first_block_index) {
			buf_pnt = buf_pnt + first_block_index;
			fread(buf_pnt, sizeof(char), (DFS_BLOCK_SIZE - first_block_index), file);
		}

		// Write from 0 to last_block_end
		else if (i == last_block_index) {
			fread(buf_pnt, sizeof(char), last_block_end, file);
		}

		// Write full block
		else {
			fread(buf_pnt, sizeof(char), DFS_BLOCK_SIZE, file);
		}

		// memset(client_datanode_request.block.content, 0, DFS_BLOCK_SIZE);
		fread(client_datanode_request.block.content, sizeof(char), DFS_BLOCK_SIZE, file); 
		
		memcpy(client_datanode_request.block.content, buffer, DFS_BLOCK_SIZE);

		client_datanode_request.op_type = 1; // datanode - create block
		strcpy(client_datanode_request.block.owner_name, response.query_result.block_list[i].owner_name);
		client_datanode_request.block.block_id = i;

		printf("dfs_client.c: modify_file: Attempting to send to datanode \n");

		if (send(datanode_socket, &client_datanode_request, sizeof(client_datanode_request), 0) == -1) // Send data
			printf("dfs_client.c: modify_file: Datanode push block send failure.\n");
		
		printf("dfs_client.c: modify_file: Successful send to datanode, closing socket \n");

		//5 - close the socket
		close(datanode_socket);		

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

	printf("dfs_client.c: push_file: namenode_socket: %i  filename: %s \n",namenode_socket,local_path);

	// Create the push request
	dfs_cm_client_req_t request; //char file_name[256]; int file_size; int req_type;//0-read, 1-write, 2-query datanodes, 3 - modify file;
	
	//Copy the name
	strcpy(request.file_name,local_path);
 	request.file_size = DFS_BLOCK_SIZE; 
 	
 	// Get file size 
 	fseek(file, 0, SEEK_END); 
	request.file_size = ftell(file);
	fseek(file, 0, SEEK_SET);

 	request.req_type = 1; // 1-write 

 	printf("dfs_client.c: push_file: Attempting to send to namenode \n");

	//TODO:fill the fields in request and send 
 	if(send(namenode_socket, &request, sizeof(request), 0) == -1 ) // Send request
 		printf("dfs_client.c: push_file: Namenode request send failure. \n"); 

 	printf("dfs_client.c: push_file: Successful Send to namenode \n");

	//TODO:Receive the response
	dfs_cm_file_res_t response; // dfs_cm_file_t query_result; // char filename[256]; dfs_cm_block_t block_list[MAX_FILE_BLK_COUNT]; int file_size; int blocknum;
	//dfs_cm_block_t: char owner_name[256]; int dn_id; int block_id; char loc_ip[16]; int loc_port; char content[DFS_BLOCK_SIZE];

	printf("dfs_client.c: push_file: Looping to recv from namenode \n");
	while(recv(namenode_socket, &response, sizeof(response), MSG_WAITALL) == -1);
	printf("dfs_client.c: push_file: Receved from namenode \n");

	char *buffer = (char *)calloc(DFS_BLOCK_SIZE, sizeof(char));
	int i, datanode_socket = 0;
	struct sockaddr_in block_dest;
	dfs_cli_dn_req_t client_datanode_request;


	printf("dfs_client.c: push_file: Beginning to send blocks to datanode one by one \n");
	//TODO: Send blocks to datanodes one by one
	for(i = 0; i < response.query_result.blocknum; i++) {
		printf("dfs_client.c: push_file: Beginning of for loop iteration (each block in file) \n");

		// 1 - create a socket for the datanode 
		datanode_socket = create_tcp_socket(); 

		// 2 - get the datanode address from the response send by namenode
		memset(&block_dest, 0, sizeof(block_dest));
		block_dest.sin_family = AF_INET;
		block_dest.sin_addr.s_addr = inet_addr(response.query_result.block_list[i].loc_ip);
		block_dest.sin_port = htons(response.query_result.block_list[i].loc_port);

		printf("dfs_client.c: push_file: Attempting to connect to datanode \n");

		// 3 - connect to the datanode
		if(datanode_socket == -1 || connect(datanode_socket, (struct sockaddr *) &block_dest, sizeof (struct sockaddr)) < 0 ) {
			if(datanode_socket != -1) {
				close (datanode_socket);
			}
			printf("dfs_client.c: push_file: Datanode connect failure. \n");
			fclose(file);
			free(buffer);
			return -1;
		}

		printf("dfs_client.c: push_file: Connected to datanode \n");

		// 4 - send data to the datanode 
		memset(client_datanode_request.block.content, 0, DFS_BLOCK_SIZE);
		fread(client_datanode_request.block.content, sizeof(char), DFS_BLOCK_SIZE, file); 
		client_datanode_request.op_type = 1; // datanode - create block
		strcpy(client_datanode_request.block.owner_name, response.query_result.block_list[i].owner_name);
		client_datanode_request.block.block_id = i;


		printf("dfs_client.c: push_file: Attempting to send to datanode \n");

		if (send(datanode_socket, &client_datanode_request, sizeof(client_datanode_request), 0) == -1) // Send data
			printf("dfs_client.c: push_file: Datanode push block send failure.\n");
		
		printf("dfs_client.c: push_file: Successful send to datanode, closing socket \n");

		//5 - close the socket
		close(datanode_socket);
	}
	
	free(buffer);
	fclose(file);
	return 0;
}

int pull_file(int namenode_socket, const char *filename)
{

	printf("Entering dfs_client.c : pull_file() \n");
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	printf("dfs_client.c: pull_file: namenode_socket: %i  filename: %s \n",namenode_socket,filename); 

	//TODO: fill the request, and send
	dfs_cm_client_req_t request; //char file_name[256]; int file_size; int req_type;//0-read, 1-write, 2-query datanodes, 3 - modify file;
	memset(&request, 0, sizeof(dfs_cm_client_req_t));
	request.req_type = 0; //important
	strcpy(request.file_name, filename);

	printf("dfs_client.c : pull_file() : about to send over namenode_socket\n");
	//send request to namenode through namenode_socket
	send(namenode_socket, &request, sizeof(request), 0);
	printf("dfs_client.c : pull_file() : Done with send\n");
	//TODO: Get the response
	dfs_cm_file_res_t response;
	assert(recv(namenode_socket, &response, sizeof(dfs_cm_file_res_t), MSG_WAITALL) != -1);
	printf("dfs_client.c : pull_file() : Done with receive\n");
	int i = 0;
	int datanode_socket = 0;
	dfs_cli_dn_req_t pull_blk_req;
	struct sockaddr_in block_dest;


	assert(response.query_result.blocknum > 0); // BUG. 
	printf("dfs_client.c : pull_file() : About to enter for loop response.query_result.blocknum: %i \n", response.query_result.blocknum);

	for (; i < response.query_result.blocknum; i++) {
	
		//create new socket
		datanode_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		assert(datanode_socket > 0);
		memset(&block_dest, 0, sizeof(block_dest));
		block_dest.sin_family = AF_INET;
		block_dest.sin_addr.s_addr = inet_addr(response.query_result.block_list[i].loc_ip);
		block_dest.sin_port = htons(response.query_result.block_list[i].loc_port);

		//Clear content to 0
		memset(response.query_result.block_list[i].content, 0, DFS_BLOCK_SIZE);

		printf("dfs_client.c : pull_file() : About to connect to datanode\n");
		//Connect to data node specified by block_lost[i]
		if (datanode_socket == -1 || connect(datanode_socket, (struct sockaddr *) &block_dest, sizeof(block_dest)) < 0) {
			if (datanode_socket != -1) {
				close(datanode_socket);

					printf("dfs_client.c : pull_file() : Datanode connection error....\n");
					return -1;
			}
		}

		printf("dfs_client.c : pull_file() : Connection to datanode a-ok\n");

		//assemble request

		pull_blk_req.op_type = 0;
		strcpy(pull_blk_req.block.owner_name, filename);
		pull_blk_req.block.block_id = i;
		printf("dfs_client.c : pull_file() : pull_blk_req.block.owner_name: %s \n",pull_blk_req.block.owner_name);

		//send request

		printf("dfs_client.c : pull_file() : about to send pull_blk_req over datanode_socket\n");
		send(datanode_socket, &pull_blk_req, sizeof(pull_blk_req), 0);

		//receive response and put into content
		recv(datanode_socket, response.query_result.block_list[i].content, DFS_BLOCK_SIZE, MSG_WAITALL);


		close(datanode_socket);

	}


	
	//TODO: Receive blocks from datanodes one by one
	
	FILE *file = fopen(filename, "wb");
	//TODO: reassemble the received blocks into the complete file
	for (i = 0; i < response.query_result.blocknum; i++) {
		fwrite(response.query_result.block_list[i].content, DFS_BLOCK_SIZE, 1, file);
	}

	fclose(file);
	return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//TODO fill the result and send 
	dfs_cm_client_req_t request;
	memset((void *)&request, 0, sizeof(request)); // Set struct memory to 0
	request.req_type = 2; // Datanode query? 

	printf("dfs_client.c: get_system_info: namenode_socket: %u  request.req_type: %i \n",namenode_socket,request.req_type);

	send(namenode_socket, &request, sizeof(request), 0); // Send request 

	//TODO: get the response
	dfs_system_status *response = (dfs_system_status *) malloc(sizeof(dfs_system_status));  
	//dfs_system_status system_status = (dfs_system_status *) malloc(sizeof(struct dfs_system_status)); 
	memset((void *)response, 0, sizeof(sizeof(dfs_system_status)));

	//response = &system_status;

	printf("dfs_client.c: get_system_info: Waiting for response from namenode \n");
	// ssize_t recv(int sockfd, void *buf, size_t len, int flags);
	if (recv(namenode_socket, (void *) response, sizeof(dfs_system_status), MSG_WAITALL) == -1){
		printf("dfs_client.c: get_system_info: ERROR: recv\n");
	}
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
	printf("dfs_client.c: send_sysinfo_request: START \n");
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return NULL;
	}
	printf("dfs_client.c: send_sysinfo_request: About to get sysinfo\n");
	dfs_system_status* ret =  get_system_info(namenode_socket);
	printf("dfs_client.c: send_sysinfo_request: About to close namenode_socket\n");
	close(namenode_socket);
	printf("dfs_client.c: send_sysinfo_request: END\n");

	return ret;
}
