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
#include "send.h"
#include "recv.h"
#include "debug.h"
#include "times.h"
#include "map.h"
#include "pthread.h"
#include "id.h"
#include "cfio_types.h"
#include "cfio_error.h"
#include "define.h"
#include "quicklist.h"
#include "addr.h"
#include "rdma_client.h"

static cfio_buf_t *buffer;

static int rank;

static double start_time;
double send_time = 0;

static cfio_buf_addr_t addr;
static int is_first_msg = 1;
static int itr = 1;

int cfio_send_init()
{
    int error, ret, server_id, client_num_of_server;

    start_time = times_cur();

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    buffer = cfio_buf_open(SEND_BUF_SIZE, &error);
    if(NULL == buffer)
    {
	error("");
	return error;
    }

    cfio_rdma_client_init(SEND_BUF_SIZE, buffer->start_addr, 
	    sizeof(cfio_buf_addr_t), (char *)&addr);
    cfio_rdma_addr_init(&addr, buffer);

    return CFIO_ERROR_NONE;
}

int cfio_send_final()
{
    cfio_msg_t *msg, *next;
    MPI_Status status;

    cfio_rdma_client_final();
	
    cfio_buf_close(buffer);

    return CFIO_ERROR_NONE;
}

/**
 * @brief: free unsed space in client buffer
 *
 * @return: 
 */
static void cfio_send_client_buf_free()
{
    printf("client %d FREE %s. \n", rank, __func__);

#ifdef SEND_ADDR
    if (!cfio_rdma_addr_client_test(&addr)) {
	cfio_rdma_client_recv_ack();
	cfio_rdma_client_wait(NULL);
    }
#endif

    cfio_rdma_addr_client_wait(&addr);
    buffer->used_addr = addr.free_addr;
    cfio_rdma_addr_init(&addr, buffer);

    cfio_rdma_addr_client_clear_signal(&addr);

#ifdef SEND_ADDR
    cfio_rdma_client_send_addr();
#else
    cfio_rdma_client_write_addr();
#endif

    cfio_rdma_client_wait(NULL);

    cfio_rdma_addr_client_wait(&addr);
    buffer->used_addr = addr.free_addr;
    cfio_rdma_addr_init(&addr, buffer);

    return;
}

int cfio_send_create(
	char *path, int cmode, int ncid)
{
    uint32_t code = FUNC_NC_CREATE;
    size_t size; 

    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_str_size(path);
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_data_size(sizeof(int));

    ensure_free_space(buffer, size, cfio_send_client_buf_free);

    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code, sizeof(uint32_t) , buffer);
    cfio_buf_pack_str(path, buffer);
    cfio_buf_pack_data(&cmode, sizeof(int), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);

    debug(DEBUG_SEND, "path = %s; cmode = %d, ncid = %d", path, cmode, ncid);

    return CFIO_ERROR_NONE;
}

/**
 *pack msg function
 **/
int cfio_send_def_dim(
	int ncid, char *name, size_t len, int dimid)
{
    uint32_t code = FUNC_NC_DEF_DIM;
    size_t size;

    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_str_size(name);
    size += cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(int));

    ensure_free_space(buffer, size, cfio_send_client_buf_free);
    
    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code, sizeof(uint32_t), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);
    cfio_buf_pack_str(name, buffer);
    cfio_buf_pack_data(&len, sizeof(size_t), buffer);
    cfio_buf_pack_data(&dimid, sizeof(int), buffer);
    
    debug(DEBUG_SEND, "ncid = %d, name = %s, len = %lu", ncid, name, len);

    return CFIO_ERROR_NONE;
}

int cfio_send_def_var(
	int ncid, char *name, cfio_type xtype,
	int ndims, int *dimids, 
	size_t *start, size_t *count, int varid)
{
    uint32_t code = FUNC_NC_DEF_VAR;
    size_t size;

    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_str_size(name);
    size += cfio_buf_data_size(sizeof(cfio_type));
    size += cfio_buf_data_array_size(ndims, sizeof(int));
    size += cfio_buf_data_array_size(ndims, sizeof(size_t));
    size += cfio_buf_data_array_size(ndims, sizeof(size_t));
    size += cfio_buf_data_size(sizeof(int));

    ensure_free_space(buffer, size, cfio_send_client_buf_free);

    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code , sizeof(uint32_t), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);
    cfio_buf_pack_str(name, buffer);
    cfio_buf_pack_data(&xtype, sizeof(cfio_type), buffer);
    cfio_buf_pack_data_array(dimids, ndims, sizeof(int), buffer);
    cfio_buf_pack_data_array(start, ndims, sizeof(size_t), buffer);
    cfio_buf_pack_data_array(count, ndims, sizeof(size_t), buffer);
    cfio_buf_pack_data(&varid, sizeof(int), buffer);

    debug(DEBUG_SEND, "ncid = %d, name = %s, ndims = %u", ncid, name, ndims);

    return CFIO_ERROR_NONE;
}

int cfio_send_put_att(
	int ncid, int varid, char *name, 
	cfio_type xtype, size_t len, void *op)
{
    uint32_t code = FUNC_PUT_ATT;
    size_t size;
    size_t att_size;

    cfio_types_size(att_size, xtype);

    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_str_size(name);
    size += cfio_buf_data_size(sizeof(cfio_type));
    size += cfio_buf_data_array_size(len, att_size);

    ensure_free_space(buffer, size, cfio_send_client_buf_free);

    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code , sizeof(uint32_t), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);
    cfio_buf_pack_data(&varid, sizeof(int), buffer);
    cfio_buf_pack_str(name, buffer);
    cfio_buf_pack_data(&xtype, sizeof(cfio_type), buffer);
    cfio_buf_pack_data_array(op, len, att_size, buffer);

    debug(DEBUG_SEND, "ncid = %d, varid = %d, name = %s, len = %lu", 
	    ncid, varid, name, len);

    return CFIO_ERROR_NONE;
}

int cfio_send_enddef(
	int ncid)
{
    uint32_t code = FUNC_NC_ENDDEF;
    size_t size;
    
    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_data_size(sizeof(int));
    
    ensure_free_space(buffer, size, cfio_send_client_buf_free);
    
    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code, sizeof(uint32_t), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);

    debug(DEBUG_SEND, "ncid = %d", ncid);
    
    return CFIO_ERROR_NONE;
}

int cfio_send_put_vara(
	int ncid, int varid, int ndims,
	size_t *start, size_t *count, 
	int fp_type, void *fp)
{
    int i;
    size_t data_len;
    uint32_t code = FUNC_NC_PUT_VARA;
    size_t size;
    
    //times_start();

    debug(DEBUG_SEND, "pack_msg_put_vara_float");
    for(i = 0; i < ndims; i ++)
    {
	debug(DEBUG_SEND, "start[%d] = %lu", i, start[i]);
    }
    for(i = 0; i < ndims; i ++)
    {
	debug(DEBUG_SEND, "count[%d] = %lu", i, count[i]);
    }
    
    data_len = 1;
    for(i = 0; i < ndims; i ++)
    {
	data_len *= count[i]; 
    }
    
    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_data_size(sizeof(int));
    size += cfio_buf_data_array_size(ndims, sizeof(size_t));
    size += cfio_buf_data_array_size(ndims, sizeof(size_t));
    size += cfio_buf_data_size(sizeof(int));
    switch(fp_type)
    {
	case CFIO_BYTE :
	    size += cfio_buf_data_array_size(data_len, 1);
	    break;
	case CFIO_CHAR :
	    size += cfio_buf_data_array_size(data_len, sizeof(char));
	    break;
	case CFIO_SHORT :
	    size += cfio_buf_data_array_size(data_len, sizeof(short));
	    break;
	case CFIO_INT :
	    size += cfio_buf_data_array_size(data_len, sizeof(int));
	    break;
	case CFIO_FLOAT :
	    size += cfio_buf_data_array_size(data_len, sizeof(float));
	    break;
	case CFIO_DOUBLE :
	    size += cfio_buf_data_array_size(data_len, sizeof(double));
	    break;
    }
	    
    ensure_free_space(buffer, size, cfio_send_client_buf_free);

    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code, sizeof(uint32_t), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);
    cfio_buf_pack_data(&varid, sizeof(int), buffer);
    cfio_buf_pack_data_array(start, ndims, sizeof(size_t), buffer);
    cfio_buf_pack_data_array(count, ndims, sizeof(size_t), buffer);
    cfio_buf_pack_data(&fp_type, sizeof(int), buffer);
    switch(fp_type)
    {
	case CFIO_BYTE :
	    cfio_buf_pack_data_array(fp, data_len, 1, buffer);
	    break;
	case CFIO_CHAR :
	    cfio_buf_pack_data_array(fp, data_len, sizeof(char), buffer);
	    break;
	case CFIO_SHORT :
	    cfio_buf_pack_data_array(fp, data_len, sizeof(short), buffer);
	    break;
	case CFIO_INT :
	    cfio_buf_pack_data_array(fp, data_len, sizeof(int), buffer);
	    break;
	case CFIO_FLOAT :
	    cfio_buf_pack_data_array(fp, data_len, sizeof(float), buffer);
	    break;
	case CFIO_DOUBLE :
	    cfio_buf_pack_data_array(fp, data_len, sizeof(double), buffer);
	    break;
    }

    //debug(DEBUG_TIME, "%f ms", times_end());
    debug(DEBUG_SEND, "ncid = %d, varid = %d, ndims = %d, data_len = %lu", 
	    ncid, varid, ndims, data_len);

    return CFIO_ERROR_NONE;
}

int cfio_send_close(
	int ncid)
{
    uint32_t code = FUNC_NC_CLOSE;
    size_t size;
    
    //times_start();
    
    size = cfio_buf_data_size(sizeof(size_t));
    size += cfio_buf_data_size(sizeof(uint32_t));
    size += cfio_buf_data_size(sizeof(int));
    
    ensure_free_space(buffer, size, cfio_send_client_buf_free);

    cfio_buf_pack_data(&size, sizeof(size_t) , buffer);
    cfio_buf_pack_data(&code, sizeof(uint32_t), buffer);
    cfio_buf_pack_data(&ncid, sizeof(int), buffer);

    //debug(DEBUG_TIME, "%f", times_end());

    return CFIO_ERROR_NONE;
}

int cfio_send_io_done()
{
//    cfio_rdma_client_poll_resume();

#ifdef SEND_ADDR
    if (!cfio_rdma_addr_client_test(&addr)) {
	cfio_rdma_client_recv_ack();
	cfio_rdma_client_wait(NULL);
    }
#endif
    cfio_rdma_addr_client_wait(&addr);

    cfio_rdma_addr_client_end_signal(&addr);

#ifdef SEND_ADDR
    cfio_rdma_client_send_addr();
#else
    cfio_rdma_client_write_addr();
#endif

    ++itr;
    return CFIO_ERROR_NONE;
}

int cfio_send_io_end()
{
    debug(DEBUG_SEND, "Start");

//    cfio_rdma_client_poll_resume();

#ifdef SEND_ADDR
    if (!cfio_rdma_addr_client_test(&addr)) {
	cfio_rdma_client_recv_ack();
	cfio_rdma_client_wait(NULL);
    }
#endif
    cfio_rdma_addr_client_wait(&addr);
    buffer->used_addr = addr.free_addr;
    cfio_rdma_addr_init(&addr, buffer);

    cfio_rdma_addr_client_clear_signal(&addr);

#ifdef SEND_ADDR
    cfio_rdma_client_send_addr();
#else
    cfio_rdma_client_write_addr();
#endif
    cfio_rdma_client_wait(NULL);

    ++itr;
    
    debug(DEBUG_SEND, "Success return");

    return CFIO_ERROR_NONE;
}

