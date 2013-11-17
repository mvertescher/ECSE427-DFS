#include "common/dfs_common.h"
#include <pthread.h>
/**
 * create a thread and activate it
 * entry_point - the function exeucted by the thread
 * args - argument of the function
 * return the handler of the thread
 */
inline pthread_t * create_thread(void * (*entry_point)(void*), void *args)
{
	//TODO: create the thread and run it
	pthread_t * thread;

	//int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);

	pthread_t t = malloc(sizeof(pthread_t));
	pthread_create(&t, NULL, entry_point, args);
	thread = &t; 

	return thread;
}

/**
 * create a socket and return
 */
int create_tcp_socket()
{
	//TODO:create the socket and return the file descriptor 
	int nn_sockfd = -1;

	//TODO: create a TCP socket
	nn_sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);	// 0

	return nn_sockfd;
}

/**
 * create the socket and connect it to the destination address
 * return the socket fd
 */
int create_client_tcp_socket(char* address, int port)
{
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();
	if (socket == INVALID_SOCKET) return 1;
	//TODO: connect it to the destination port

	struct sockaddr_in client_addr;
	
	memset((void *)&client_addr, 0, sizeof(client_addr));
	client_addr.sin_family = AF_INET;
	client_addr.sin_addr.s_addr = inet_addr(address);
	client_addr.sin_port = htons(port);


	printf("dfs_common.c: create_client_tcp_socket: Attempting to connect\n");
	// Connect 	
	if ( socket == -1 || connect(socket, (struct sockaddr *) &client_addr, sizeof(client_addr)) < 0 ) { 
		printf("dfs_common.c: create_client_tcp_socket: Client cannot connect to server\n"); return -1 ;
	}
	printf("dfs_common.c: create_client_tcp_socket: Connection finished: socket %i \n",socket);

	return socket;
}

/**
 * create a socket listening on the certain local port and return
 */
int create_server_tcp_socket(int port)
{
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();
	if (socket == INVALID_SOCKET) return 1;
	//TODO: listen on local port

	struct sockaddr_in server_addr;
	
	memset((void *)&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(port);
	
	// Bind and listen
	bind(socket, (struct sockaddr *) &server_addr, sizeof(server_addr));
	listen(socket, 5);

	return socket;
}

/**
 * socket - connecting socket
 * data - the buffer containing the data
 * size - the size of buffer, in byte
 */
void send_data(int socket, void* data, int size)
{
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	//TODO: send data through socket
}

/**
 * receive data via socket
 * socket - the connecting socket
 * data - the buffer to store the data
 * size - the size of buffer in byte
 */
void receive_data(int socket, void* data, int size)
{
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	//TODO: fetch data via socket
}
