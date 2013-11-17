#ifndef DFS_CLIENT_H
#define DFS_CLIENT_H

#include <string.h>
#include "common/dfs_common.h"

int connect_to_nn(char *nn_ip, int port);
/**
 * upload/fownload the file to/from the distributed file system
 * sockfd - the socket connecting to the namenode
 * filename - the name of the file to be pushed or pulled
 */
int push_file(int nn_sockfd, const char *filename);
int pull_file(int nn_sockfd, const char *filename);
int send_file_request(char **argv, char *file_name, int op_type);
dfs_system_status *get_system_info(int nn_socket);
dfs_system_status *send_sysinfo_request(char **argv);
#endif
