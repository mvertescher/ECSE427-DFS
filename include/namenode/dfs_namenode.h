#ifndef DFS_NAMENODE_H
#define DFS_NAMENODE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "common/dfs_common.h"

int mainLoop();
int start();//start the namenode
//int terminate();//terminate the namenode process
int register_datanode(int register_node);//add a new datanode
int requests_dispatcher(int client_socket, dfs_cm_client_req_t request);//dispatching a request to proper handler
int get_file_receivers(int client_socket, dfs_cm_client_req_t request);
int get_file_location(int client_socket, dfs_cm_client_req_t request);
int delete_file(int client_socket, dfs_cm_client_req_t request);
#endif
