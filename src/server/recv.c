/****************************************************************************
 *       Filename:  msg.c
 *
 *    Description:  implement for msg between IO process adn IO forwardinging process
 *
 *        Version:  1.0
 *        Created:  12/13/2011 03:11:48 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "msg.h"
#include "recv.h"
#include "send.h"
#include "debug.h"
#include "times.h"
#include "map.h"
#include "pthread.h"
#include "id.h"
#include "cfio_types.h"
#include "cfio_error.h"
#include "define.h"

//#define DEBUG

static cfio_msg_t *msg_head;
//use two buffer swap, in client :writer for pack, reader for send
static cfio_buf_t **buffer;
static int rank;
static int client_num;
/* index used when call cfio_recv_get_first */
static int client_get_index = 0;

int cfio_recv_init()
{
    int i, error;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    client_num = cfio_map_get_client_num_of_server(rank);

    msg_head = malloc(client_num * sizeof(cfio_msg_t));
    if(NULL == msg_head)
    {
	return CFIO_ERROR_MALLOC;
    }
    for(i = 0; i < client_num; i ++)
    {
	INIT_QLIST_HEAD(&(msg_head[i].link));
    }

    buffer = malloc(client_num *sizeof(cfio_buf_t*));
    if(NULL == buffer)
    {
	return CFIO_ERROR_MALLOC;
    }
    for(i = 0; i < client_num; i ++)
    {
	buffer[i] = cfio_buf_open(RECV_BUF_SIZE / client_num, &error);
	if(NULL == buffer[i])
	{
	    error("");
	    return error;
	}
    }

    return CFIO_ERROR_NONE;
}

int cfio_recv_final()
{
    cfio_msg_t *msg, *next;
    MPI_Status status;
    int i = 0;

    if(msg_head != NULL)
    {
	free(msg_head);
    }

    if(buffer != NULL)
    {
	for(i = 0; i < client_num; i ++)
	{
	    cfio_buf_close(buffer[i]);
	}
	free(buffer);
    }

    return CFIO_ERROR_NONE;
}

int cfio_recv_add_msg(int client_rank, int size, char *data, uint32_t *func_code, int itr)
{
    int client_index;
    cfio_msg_t *msg;

    client_index = cfio_map_get_client_index_of_server(client_rank);

    if (cfio_buf_is_empty(buffer[client_index])) {
	cfio_buf_clear(buffer[client_index]);
    }

    if(is_free_space_enough(buffer[client_index], size)
	    == CFIO_BUF_FREE_SPACE_NOT_ENOUGH) {
#ifdef DEBUG
	printf("itr %d server %d client %d buffer_info: start, used, free: %lu, %lu, %lu .\n", 
		itr, rank, client_rank, (uintptr_t)(buffer[client_index]->start_addr),
		(uintptr_t)(buffer[client_index]->used_addr),
		(uintptr_t)(buffer[client_index]->free_addr));
#endif
	return CFIO_RECV_BUF_FULL;
    }

    *func_code = *((uint32_t *)(data + sizeof(size_t)));

#ifdef DEBUG
    printf("itr %d server %d client %d func_code %d msg_size %d \n", itr, rank, client_rank, *func_code, size);
#endif

    if (!size) {
#ifdef DEBUG
	printf("itr %d server %d client %d buffer_info: start, used, free: %lu, %lu, %lu .\n", 
		itr, rank, client_rank, (uintptr_t)(buffer[client_index]->start_addr),
		(uintptr_t)(buffer[client_index]->used_addr),
		(uintptr_t)(buffer[client_index]->free_addr));
#endif
	return CFIO_ERROR_UNEXPECTED_MSG;
    }

    if (FUNC_FINAL == *func_code) {
	return CFIO_ERROR_NONE;
    } else if (FUNC_NC_CLOSE == *func_code) {
#ifdef DEBUG
	printf("itr %d server %d client %d buffer_info: start, end, used, free: %lu, %lu, %lu, %lu .\n", 
		itr, rank, client_rank, 
		(uintptr_t)(buffer[client_index]->start_addr),
		(uintptr_t)(buffer[client_index]->start_addr + buffer[client_index]->size),
		(uintptr_t)(buffer[client_index]->used_addr),
		(uintptr_t)(buffer[client_index]->free_addr));
#endif
    }

    memcpy(buffer[client_index]->free_addr, data, size);

    msg = cfio_msg_create();
    msg->addr = buffer[client_index]->free_addr;
    msg->size = size;
    msg->src = client_rank;
    msg->dst = rank;
    msg->func_code = *func_code; 

    use_buf(buffer[client_index], size);
    qlist_add_tail(&(msg->link), &(msg_head[client_index].link));

    return CFIO_ERROR_NONE;
}

cfio_msg_t *cfio_recv_get_first()
{
    cfio_msg_t *_msg = NULL, *msg;
    qlist_head_t *link;
    size_t size;

    debug(DEBUG_RECV, "client_get_index = %d", client_get_index);
    if(qlist_empty(&(msg_head[client_get_index].link)))
    {
	link = NULL;
    }else
    {
	link = msg_head[client_get_index].link.next;
    }

    if(NULL == link)
    {
	msg = NULL;
    }else
    {
	msg = qlist_entry(link, cfio_msg_t, link);
	cfio_recv_unpack_msg_size(msg, &size);
	if(msg->size == size) //only contain one single msg
	{
	    qlist_del(link);
	    _msg = msg;
	}else
	{
	    msg->size -= size;
	    _msg = cfio_msg_create();
	    _msg->addr = msg->addr;
	    _msg->src = msg->src;
	    _msg->dst = msg->dst;
	    msg->addr += size;
	}
	client_get_index = (client_get_index + 1) % client_num;
    }

    if(_msg != NULL)
    {
	debug(DEBUG_RECV, "get msg size : %lu", _msg->size);
    }

    return _msg;
}

/**
 *unpack msg function
 **/
int cfio_recv_unpack_msg_size(cfio_msg_t *msg, size_t *size)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    debug(DEBUG_RECV, "client_index = %d", client_index);

    assert(check_used_addr(msg->addr, buffer[client_index]));
    
    buffer[client_index]->used_addr = msg->addr;
    cfio_buf_unpack_data(size, sizeof(size_t), buffer[client_index]);

    debug(DEBUG_RECV, "size : %lu", *size);

    return CFIO_ERROR_NONE;
}

int cfio_recv_unpack_func_code(cfio_msg_t *msg, uint32_t *func_code)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    debug(DEBUG_RECV, "client_index = %d", client_index);

    //if(msg->addr == buffer->start_addr)
    //{
    //    debug_mark(DEBUG_RECV);
    //}

    cfio_buf_unpack_data(func_code, sizeof(uint32_t), buffer[client_index]);

    return CFIO_ERROR_NONE;
}

int cfio_recv_unpack_create(
	cfio_msg_t *msg,
	char **path, int *cmode, int *ncid)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    
    cfio_buf_unpack_str(path, buffer[client_index]);
    cfio_buf_unpack_data(cmode, sizeof(int), buffer[client_index]);
    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);

    debug(DEBUG_RECV, "path = %s; cmode = %d, ncid = %d", *path, *cmode, *ncid);

    return CFIO_ERROR_NONE;
}

int cfio_recv_unpack_def_dim(
	cfio_msg_t *msg,
	int *ncid, char **name, size_t *len, int *dimid)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);

    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);
    cfio_buf_unpack_str(name, buffer[client_index]);
    cfio_buf_unpack_data(len, sizeof(size_t), buffer[client_index]);
    cfio_buf_unpack_data(dimid, sizeof(int), buffer[client_index]);
    
    debug(DEBUG_RECV, "ncid = %d, name = %s, len = %lu", *ncid, *name, *len);

    return CFIO_ERROR_NONE;
}

int cfio_recv_unpack_def_var(
	cfio_msg_t *msg,
	int *ncid, char **name, cfio_type *xtype,
	int *ndims, int **dimids, 
	size_t **start, size_t **count, int *varid)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    
    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);
    cfio_buf_unpack_str(name, buffer[client_index]);
    cfio_buf_unpack_data(xtype, sizeof(cfio_type), buffer[client_index]);
    cfio_buf_unpack_data_array((void **)dimids, ndims, 
	    sizeof(int), buffer[client_index]);
    cfio_buf_unpack_data_array((void **)start, ndims, 
	    sizeof(size_t), buffer[client_index]);
    cfio_buf_unpack_data_array((void **)count, ndims, 
	    sizeof(size_t), buffer[client_index]);
    cfio_buf_unpack_data(varid, sizeof(int), buffer[client_index]);
    
    debug(DEBUG_RECV, "ncid = %d, name = %s, ndims = %u", *ncid, *name, *ndims);

    return CFIO_ERROR_NONE;
}
int cfio_recv_unpack_put_att(
	cfio_msg_t *msg,
	int *ncid, int *varid, char **name, 
	cfio_type *xtype, int *len, void **op)
{
    size_t att_size;
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    

    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);
    cfio_buf_unpack_data(varid, sizeof(int), buffer[client_index]);
    cfio_buf_unpack_str(name, buffer[client_index]);
    cfio_buf_unpack_data(xtype, sizeof(cfio_type), buffer[client_index]);
    
    cfio_types_size(att_size, *xtype);
    cfio_buf_unpack_data_array(op, len, att_size, buffer[client_index]);

    debug(DEBUG_RECV, "ncid = %d, varid = %d, name = %s, len = %d",
	    *ncid, *varid, *name, *len);

    return CFIO_ERROR_NONE;
}

int cfio_recv_unpack_enddef(
	cfio_msg_t *msg,
	int *ncid)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    
    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);
    debug(DEBUG_RECV, "ncid = %d", *ncid);

    return CFIO_ERROR_NONE;
}

int cfio_recv_unpack_put_vara(
	cfio_msg_t *msg,
	int *ncid, int *varid, int *ndims, 
	size_t **start, size_t **count,
	int *data_len, int *fp_type, char **fp)
{
    int i;
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    

    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);
    cfio_buf_unpack_data(varid, sizeof(int), buffer[client_index]);
    
    cfio_buf_unpack_data_array((void**)start, ndims, sizeof(size_t), 
	    buffer[client_index]);
    cfio_buf_unpack_data_array((void**)count, ndims, sizeof(size_t),
	    buffer[client_index]);

//    cfio_buf_unpack_data_array_ptr((void**)fp, data_len, 
//	    sizeof(float), buffer[client_index]);
//
    cfio_buf_unpack_data(fp_type, sizeof(int), buffer[client_index]);
    switch(*fp_type)
    {
	case CFIO_BYTE :
	    cfio_buf_unpack_data_array((void**)fp, data_len, 1, 
		    buffer[client_index]);
	    break;
	case CFIO_CHAR :
	    cfio_buf_unpack_data_array((void**)fp, data_len, 1, 
		    buffer[client_index]);
	    break;
	case CFIO_SHORT :
	    cfio_buf_unpack_data_array((void**)fp, data_len, sizeof(short), 
		    buffer[client_index]);
	    break;
	case CFIO_INT :
	    cfio_buf_unpack_data_array((void**)fp, data_len, sizeof(int), 
		    buffer[client_index]);
	    break;
	case CFIO_FLOAT :
	    cfio_buf_unpack_data_array((void**)fp, data_len, sizeof(float), 
		    buffer[client_index]);
	    break;
	case CFIO_DOUBLE :
	    cfio_buf_unpack_data_array((void**)fp, data_len, sizeof(double), 
		    buffer[client_index]);
	    break;
    }

    debug(DEBUG_RECV, "ncid = %d, varid = %d, ndims = %d, data_len = %u", 
	    *ncid, *varid, *ndims, *data_len);
    //debug(DEBUG_RECV, "fp[0] = %f", (*fp)[0]); 
    
    return CFIO_ERROR_NONE;
}
	
int cfio_recv_unpack_close(
	cfio_msg_t *msg,
	int *ncid)
{
    int client_index;

    client_index = cfio_map_get_client_index_of_server(msg->src);
    
    cfio_buf_unpack_data(ncid, sizeof(int), buffer[client_index]);
    debug(DEBUG_RECV, "ncid = %d", *ncid);

    return CFIO_ERROR_NONE;
}

