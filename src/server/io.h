/*
 * =====================================================================================
 *
 *       Filename:  io.h
 *
 *    Description: unmap the io_forward op into a real operation 
 *
 *        Version:  1.0
 *        Created:  12/20/2011 04:54:26 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  drealdal (zhumeiqi), meiqizhu@gmail.com
 *        Company:  Tsinghua University, HPC
 *
 * =====================================================================================
 */
#ifndef _IO_H
#define _IO_H

#include "msg.h"

#define IO_HASH_TABLE_SIZE 32

/* the msg is delt, the buffer could be reused inmmediately */
#define DEALT_MSG 2
/* the msg is put into the queue, the buffer should be keep util
 * the write to free it */
#define ENQUEUE_MSG 3
#define IMM_MSG 4

#define ATT_NAME_SUB_AMOUNT	    "sub_amount"
#define ATT_NAME_START		    "start"

typedef struct
{
    int func_code;
    int client_nc_id;
    int client_dim_id;
    int client_var_id;
}cfio_io_key_t;

typedef struct
{
    int func_code;
    int client_nc_id;
    int client_dim_id;
    int client_var_id;

    uint8_t *client_bitmap;

    qlist_head_t hash_link;
    //qlist_head_t queue_link;
}cfio_io_val_t;

/**
 * @brief: initialize
 *
 * @return: error code
 */
int cfio_io_init();
/**
 * @brief: finalize
 *
 * @return: error code
 */
int cfio_io_final();
int cfio_io_reader_done(int client_id, int *server_done);
int cfio_io_writer_done(int client_id, int *server_done);
int cfio_io_create(cfio_msg_t *msg);
int cfio_io_def_dim(cfio_msg_t *msg);
int cfio_io_def_var(cfio_msg_t *msg);
int cfio_io_enddef(cfio_msg_t *msg);
int cfio_io_put_vara(cfio_msg_t *msg);
int cfio_io_close(cfio_msg_t *msg);

#endif
