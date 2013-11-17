#ifndef DFS_DATANODE_H
#define DFS_DATANODE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/dfs_common.h"
#include "datanode/ext.h"

int start();//start the datanode
int mainLoop();//main service loop

void requests_dispatcher(int client_socket, dfs_cli_dn_req_t request);
int read_block(int client_socket, const dfs_cli_dn_req_t *request);
int create_block(const dfs_cli_dn_req_t *request);
#endif 
