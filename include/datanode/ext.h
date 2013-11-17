#ifndef EXT_H
#define EXT_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int get_block_file_path(char *path, const char *filename, int block_id);
void ext_init_local_fs(char* fd);
void ext_close_local_fs();
int ext_read_block(const char *filename, int block_id, void *buf);
int ext_write_block(const char *filename, int block_id, void *buf);

#endif
