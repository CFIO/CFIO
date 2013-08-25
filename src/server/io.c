/*
 * =====================================================================================
 *
 *       Filename:  unmap.c
 *
 *    Description:  map an io_forward operation into an 
 *    		    real operation 
 *
 *        Version:  1.0
 *        Created:  12/20/2011 04:53:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  drealdal (zhumeiqi), meiqizhu@gmail.com
 *        Company:  Tsinghua University, HPC
 *
 * =====================================================================================
 */
#include <pnetcdf.h>
#include <assert.h>
#include <string.h>

#include "mpi.h"

#include "io.h"
#include "id.h"
#include "msg.h"
#include "buffer.h"
#include "debug.h"
#include "quickhash.h"
#include "cfio_types.h"
#include "cfio_error.h"
#include "map.h"
#include "define.h"
#include "times.h"

static struct qhash_table *io_table;
static int server_id;
//static double start_time;
//static int file_num = 0;
//static double write_time = 0.0;

static int _compare(void *key, struct qhash_head *link)
{
    assert(NULL != key);
    assert(NULL != link);

    cfio_io_val_t *val = qlist_entry(link, cfio_io_val_t, hash_link);

    if(0 == memcmp(key, val, sizeof(cfio_io_key_t)))
    {
	return 1;
    }

    return 0;
}

static int _hash(void *key, int table_size)
{
    cfio_io_key_t *_key = key;
    int a, b, c;

    a = _key->func_code; b = _key->client_nc_id; c = _key->client_dim_id;
    mix(a, b, c);
    a += _key->client_var_id;
    final(a, b, c);

    int h = (int)(c & (table_size - 1));

    return h;
}

static void _free(cfio_io_val_t *val)
{
    if(NULL != val)
    {
	free(val);
	val = NULL;
    }
}

/**
 * @brief: add a new client id in the io request bitmap, which means an IO request
 *	   is recived from the client
 *
 * @param bitmap: 
 * @param client_id: 
 */
static inline void _add_bitmap(uint8_t *bitmap, int client_id)
{
    int i, j;
    int client_index;

    assert(client_id < cfio_map_get_client_amount());
    assert(bitmap != NULL);
    
    client_index = cfio_map_get_client_index_of_server(client_id);

    i = client_index >> 3;
    j = client_index - (i << 3);

    bitmap[i] |= (1 << j);
}

/**
 * @brief: judge whether the bitmap is full, if a bitmap is full, the io request
 *	   it is refer to can be handled by server
 *
 * @param bitmap: 
 *
 * @return: 1 if bitmap is full
 */
static inline int _bitmap_full(uint8_t *bitmap)
{
    int is_full;
    int i;
    int head, tail;
    int client_num;
    
    client_num = cfio_map_get_client_num_of_server(server_id);
    head = client_num >> 3;
    tail = client_num - (head << 3);

    for(i = 0; i < head; i ++)
    {
	if(bitmap[i] != 255)
	{
	    return 0;
	}
    }
    
    if(bitmap[head] != ((1 << tail) - 1))
    {
	return 0;
    }

    return 1;
}

/**
 * @brief: recv a io request from client , so put a new io request into the hash
 *	   table
 *
 * @param client_id: client id
 * @param func_code: function code of the io request
 * @param client_nc_id: client nc id
 * @param client_dim_id: client dimension id, if the io request is variable 
 *			 function, the arg will be set to 0
 * @param client_var_id: client variable id, if the io request is dimension 
 *		         function, the arg will be set to 0
 * @param io_info: the put hash value, store all informtion of the IO request
 *
 * @return: error code
 */
static inline int _recv_client_io(
	int client_id, int func_code,
	int client_nc_id, int client_dim_id, int client_var_id,
	cfio_io_val_t **io_info)
{
    cfio_io_key_t key;
    cfio_io_val_t *val;
    qlist_head_t *link;
    int client_num;

    key.func_code = func_code;
    key.client_nc_id = client_nc_id;
    key.client_dim_id = client_dim_id;
    key.client_var_id = client_var_id;

    client_num = cfio_map_get_client_num_of_server(server_id);

    if(NULL == (link = qhash_search(io_table, &key)))
    {
	val = malloc(sizeof(cfio_io_val_t));

	memcpy(val, &key, sizeof(cfio_io_key_t));
	val->client_bitmap = malloc((client_num >> 3) + 1);
	memset(val->client_bitmap, 0, (client_num >> 3) + 1);
	qhash_add(io_table, &key, &(val->hash_link));

    }else
    {
	val = qlist_entry(link, cfio_io_val_t, hash_link);
    }
    _add_bitmap(val->client_bitmap, 
	    client_id);
    *io_info = val;

    return CFIO_ERROR_NONE;
}

/**
 * @brief: remove an handled io request from hash table
 *
 * @param io_info: the handled io request
 *
 * @return: error code
 */
static inline int _remove_client_io(
	cfio_io_val_t *io_info)
{
    assert(io_info != NULL);

    qhash_del(&io_info->hash_link);

    if(NULL != io_info->client_bitmap) 
    {
	free(io_info->client_bitmap);
	io_info->client_bitmap = NULL;
    }

    free(io_info);

    return CFIO_ERROR_NONE;
}

static void _inc_src_index(
	const int ndims, const size_t ele_size, 
	const size_t *dst_dims_len, char **dst_addr, 
	const size_t *src_dims_len, size_t *src_index)
{
    int dim;
    size_t sub_size, last_sub_size;

    dim = ndims - 1;
    src_index[dim] ++;

    sub_size = ele_size;
    while(src_index[dim] >= src_dims_len[dim])
    {
	*dst_addr -= (src_dims_len[dim] - 1) * sub_size;
	src_index[dim] = 0;
	sub_size *= dst_dims_len[dim];
	dim --;
	src_index[dim] ++;
    }

    *dst_addr += sub_size;
}

/**
 * @brief: put the src data array into the dst data array, src and dst both are
 *	sub-array of a total data array
 *
 * @param ndims: number of dimensions for the variable
 * @param ele_size: size of each element in the variable array
 * @param dst_start: start index of the dst data array
 * @param dst_count: count of teh dst data array
 * @param dst_data: pointer to the dst data array
 * @param src_start: start index of the src data array
 * @param src_count: count of teh src data array
 * @param src_data: pointer to the src data array
 */
static int _put_var(
	int ndims, size_t ele_size,
	size_t *dst_start, size_t *dst_count, char *dst_data, 
	size_t *src_start, size_t *src_count, char *src_data)
{
    int i;
    size_t src_len;
    size_t dst_offset, sub_size;
    size_t *src_index;

    //float *_data = src_data;
    //for(i = 0; i < 4; i ++)
    //{
    //    printf("%f, ", _data[i]);
    //}
    //printf("\n");

    assert(NULL != dst_start);
    assert(NULL != dst_count);
    assert(NULL != dst_data);
    assert(NULL != src_start);
    assert(NULL != src_count);
    assert(NULL != src_data);

    src_len = 1;
    for(i = 0; i < ndims; i ++)
    {
	src_len *= src_count[i];
    }

    src_index = malloc(sizeof(size_t) * ndims);
    if(NULL == src_index)
    {
	error("malloc for src_index fail.");
	return CFIO_ERROR_MALLOC;
    }
    for(i = 0; i < ndims; i ++)
    {
	src_index[i] = 0;
    }

    sub_size = 1;
    dst_offset = 0;
    for(i = ndims - 1; i >= 0; i --)
    {
	dst_offset += (src_start[i] - dst_start[i]) * sub_size;
	sub_size *= dst_count[i];
    }
    //debug(DEBUG_ID, "dst_offset = %d", dst_offset);
    dst_data += ele_size * dst_offset;

    for(i = 0; i < src_len - 1; i ++)
    {
	memcpy(dst_data, src_data, ele_size);
	_inc_src_index(ndims, ele_size, dst_count, &dst_data, 
		src_count, src_index);
	src_data += ele_size;
    }
    memcpy(dst_data, src_data, ele_size);

    return CFIO_ERROR_NONE;
}


static inline int _handle_def(cfio_id_val_t *val)
{
    int ret, i;
    cfio_id_nc_t *nc;
    cfio_id_dim_t *dim;
    cfio_id_var_t *var;
    cfio_id_att_t *att;
    size_t data_size, ele_size;
    int *start;

    if(NULL != val->dim)
    {
	dim = val->dim;
	cfio_id_get_nc(val->client_nc_id, &nc);
	assert(nc->nc_id != CFIO_ID_NC_INVALID);
	dim->nc_id = nc->nc_id;
	debug(DEBUG_IO, "dim_len = %d", dim->dim_len);
#ifndef SVR_NO_IO
	ret = ncmpi_def_dim(nc->nc_id, dim->name, dim->global_dim_len, &dim->dim_id);
#else
	ret = NC_NOERR;
	dim->dim_id = 10;
#endif
	return CFIO_ERROR_NONE;
    }

    if(NULL != val->var)
    {
	var = val->var;
	for(i = 0; i < var->ndims; i ++)
	{
	    cfio_id_get_dim(val->client_nc_id, var->dim_ids[i], &dim);
	    var->dim_ids[i] = dim->dim_id;
	}
	var->nc_id = dim->nc_id;
	debug(DEBUG_IO, "Def var : cfio_type(%d), nc_type(%d)", 
		var->data_type, cfio_type_to_nc(var->data_type));
#ifndef SVR_NO_IO
	ret = ncmpi_def_var(var->nc_id, var->name, cfio_type_to_nc(var->data_type), var->ndims,
		var->dim_ids, &var->var_id);
#else
	ret = NC_NOERR;
	var->var_id = 10;
#endif
	qlist_for_each_entry(att, var->att_head, link)
	{
	    switch(att->xtype)
	    {
		case CFIO_CHAR :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_text(var->nc_id, var->var_id, att->name, att->len, att->data);
#else
		    ret = NC_NOERR;
#endif
		    break;
		case CFIO_INT :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_int(nc->nc_id, var->var_id, att->name, 
			    cfio_type_to_nc(att->xtype), att->len, (const int*)att->data);
#else
		    ret = NC_NOERR;
#endif
		    break;
		case CFIO_FLOAT :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_float(nc->nc_id, var->var_id, att->name, cfio_type_to_nc(att->xtype), att->len, (const float*)att->data);
#else
		    ret = NC_NOERR;
#endif
		    break;
		case CFIO_DOUBLE :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_double(nc->nc_id, var->var_id, att->name, cfio_type_to_nc(att->xtype), att->len, (const double*)att->data);
#else
		    ret = NC_NOERR;
#endif
		    break;
	    }
	    if(ret != NC_NOERR)
	    {
		error("put var(%s) attr(%s) error(%s)",
			var->name, att->name, ncmpi_strerror(ret));
		return CFIO_ERROR_NC;
	    }

	}
	
	return CFIO_ERROR_NONE;
    }

    error("no def can do , this is impossible.");
    return CFIO_ERROR_NC;
}
/**
 * @brief: update a var's start and count, still store in cur_start and cur_count
 *
 * @param ndims: number of dim for the var
 * @param cur_start: current start of the var
 * @param cur_count: current count of the var
 * @param new_start: new start which is to be updated into cur_start 
 * @param new_count: new count which is to be updated into cur_count
 */
void _update_start_and_count(int ndims, 
	size_t *cur_start, size_t *cur_count,
	size_t *new_start, size_t *new_count)
{
    assert(NULL != cur_start);
    assert(NULL != cur_count);
    assert(NULL != new_start);
    assert(NULL != new_count);

    int min_start, max_end;
    int i;

    for(i = 0; i < ndims; i ++)
    {
	min_start = (cur_start[i] < new_start[i]) ? cur_start[i]:new_start[i];
	max_end = cur_start[i] + cur_count[i] > new_start[i] + new_count[i] ? 
	    cur_start[i] + cur_count[i] : new_start[i] + new_count[i];
	cur_start[i] = min_start;
	cur_count[i] = max_end - min_start;
    }
}
/* merge var data */
void _merge_var_data(
	cfio_id_var_t *var, size_t *start, size_t *count, char **_data)
{
    int i;
    size_t data_size, ele_size;
    char *data;
    
    debug(DEBUG_IO, "_merge_var_data");
    debug(DEBUG_IO, "ndims = %d\n", var->ndims);
	
    assert(var->recv_data != NULL);

    for(i = 0; i < var->ndims; i ++)
    {
	assert(var->recv_data[0].start != NULL);
	assert(var->recv_data[0].count != NULL);
	debug(DEBUG_IO, "start = %lu\n", var->recv_data[0].start[i]);
	debug(DEBUG_IO, "count = %lu\n", var->recv_data[0].count[i]);
	start[i] = var->recv_data[0].start[i];
	count[i] = var->recv_data[0].count[i];
    }
    
    for(i = 1; i < var->client_num; i ++)
    {
	_update_start_and_count(var->ndims,start, count,
		var->recv_data[i].start, var->recv_data[i].count);
    }
	
    data_size = 1;
    for(i = 0; i < var->ndims; i ++)
    {
	data_size *= count[i];
    }
    cfio_types_size(ele_size, var->data_type);
    data = malloc(ele_size * data_size);

    debug(DEBUG_IO, "malloc for data, size = %lu * %lu", 
	    ele_size ,  data_size);

    for(i = 0; i < var->client_num; i ++)
    {
	_put_var(var->ndims, ele_size, 
		start, count, data,
		var->recv_data[i].start, var->recv_data[i].count,
		var->recv_data[i].buf);

	free(var->recv_data[i].buf);	
	var->recv_data[i].buf = NULL;	
	free(var->recv_data[i].start);	
	var->recv_data[i].start = NULL;	
	free(var->recv_data[i].count);	
	var->recv_data[i].count = NULL;	
	
    }
    
    *_data = data;	
}

int cfio_io_init()
{
    io_table = qhash_init(_compare, _hash, IO_HASH_TABLE_SIZE);
    MPI_Comm_rank(MPI_COMM_WORLD, &server_id);

    //start_time = times_cur();
    return CFIO_ERROR_NONE;
}

int cfio_io_final()
{
    if(NULL != io_table)
    {
	qhash_destroy_and_finalize(io_table, cfio_io_val_t, hash_link, _free);
	io_table = NULL;
    }

    return CFIO_ERROR_NONE;
}

int cfio_io_reader_done(int client_id, int *server_done)
{
    int func_code = FUNC_READER_FINAL;
    cfio_io_val_t *io_info;

    _recv_client_io(client_id, func_code, 0, 0, 0, &io_info);

    if(_bitmap_full(io_info->client_bitmap))
    {
	_remove_client_io(io_info);
	*server_done = 1;
    }

    return CFIO_ERROR_NONE;	
}

int cfio_io_writer_done(int client_id, int *server_done)
{
    int func_code = FUNC_WRITER_FINAL;
    cfio_io_val_t *io_info;

    _recv_client_io(client_id, func_code, 0, 0, 0, &io_info);

    if(_bitmap_full(io_info->client_bitmap))
    {
	_remove_client_io(io_info);
	*server_done = 1;
    }

    return CFIO_ERROR_NONE;	
}

int cfio_io_create(cfio_msg_t *msg)
{
    int ret, cmode;
    char *_path = NULL;
    int nc_id, client_nc_id;
    cfio_id_nc_t *nc;
    //cfio_io_val_t *io_info;
    int return_code;
    int func_code = FUNC_NC_CREATE;
    char *path;
    int sub_file_amount;
    int client_id = msg->src;

    //printf("create %d time : %f\n", (file_num ++) % 4, times_cur() - start_time);
    //printf("create %d time : %f\n", file_num ++, times_cur() - start_time);

    cfio_recv_unpack_create(msg, &_path,&cmode, &client_nc_id);

#ifdef SVR_UNPACK_ONLY
    if(_path != NULL)
    {
	free(_path);
	_path = NULL;
    }
    return CFIO_ERROR_NONE;
#endif

    /* TODO  */
    path = malloc(strlen(_path) + 32);
    sprintf(path, "%s", _path);

    //_recv_client_io(client_id, func_code, client_nc_id, 0, 0, &io_info);

    if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
    {
	cfio_id_map_nc(client_nc_id, CFIO_ID_NC_INVALID);
	//if(_bitmap_full(io_info->client_bitmap))
	//{
#ifndef SVR_NO_IO
	ret = ncmpi_create(cfio_map_get_server_comm(), path, cmode, MPI_INFO_NULL, &nc_id);
#else
	ret = NC_NOERR;
	nc_id = NC_NOERR;
#endif
	if(ret != NC_NOERR)
	{
	    error("Error happened when open %s error(%s)", 
		    path, ncmpi_strerror(ret));
	    return_code = CFIO_ERROR_NC;

	    goto RETURN;
	}
	//put attr of sub_amount

	if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
	{
	    assert(1);
	}else
	{
	    assert(CFIO_ID_NC_INVALID == nc->nc_id);

	    nc->nc_id = nc_id;
	}
    }

    //_remove_client_io(io_info);
    debug(DEBUG_IO, "nc create(%s) success", path);
    //}

    return_code = CFIO_ERROR_NONE;

RETURN:
    if(NULL != path)
    {
	free(path);
	path = NULL;
    }
    if(NULL != _path)
    {
	free(_path);
	path = NULL;
    }
    return return_code;
}

int cfio_io_def_dim(cfio_msg_t *msg)
{
    int ret = 0;
    int dim_id;
    int client_nc_id, client_dim_id;
    cfio_id_nc_t *nc;
    cfio_id_dim_t *dim;
    cfio_io_val_t *io_info;
    size_t len;
    char *name = NULL;
    int client_id = msg->src;

    int func_code = FUNC_NC_DEF_DIM;
    int return_code;

    cfio_recv_unpack_def_dim(msg, &client_nc_id, &name, &len, &client_dim_id);
    
    debug(DEBUG_CFIO, "ncid = %d, name = %s, len = %lu",
	    client_nc_id, name, len);

#ifdef SVR_UNPACK_ONLY
    if(name != NULL)
    {
	free(name);
	name = NULL;
    }
    return CFIO_ERROR_NONE;
#endif

    //_recv_client_io(
    //        client_id, func_code, client_nc_id, client_dim_id, 0, &io_info);
	
    if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
    {
	return_code = CFIO_ERROR_INVALID_NC;
	debug(DEBUG_IO, "Invalid NC ID.");
	goto RETURN;
    }

    if(CFIO_ID_HASH_GET_NULL == 
            cfio_id_get_dim(client_nc_id, client_dim_id, &dim))
    {
        cfio_id_map_dim(client_nc_id, client_dim_id, CFIO_ID_NC_INVALID, 
        	CFIO_ID_DIM_INVALID, name, len);
    }else
    {
	free(name);
    }

    //if(_bitmap_full(io_info->client_bitmap))
    //{
    //    if(CFIO_ID_NC_INVALID == nc->nc_id)
    //    {
    //        return_code = CFIO_ERROR_INVALID_NC;
    //        debug(DEBUG_IO, "Invalid NC ID.");
    //        goto RETURN;
    //    }
    //    if(nc->nc_status != DEFINE_MODE)
    //    {
    //        return_code = CFIO_ERROR_NC_NOT_DEFINE;
    //        debug(DEBUG_IO, "Only can define dim in DEFINE_MODE.");
    //        goto RETURN;
    //    }

    //    ret = def_dim(nc->nc_id,name,len,&dim_id);
    //    if( ret != NC_NOERR )
    //    {
    //        error("def dim(%s) error(%s)",name,ncmpi_strerror(ret));
    //        return_code = CFIO_ERROR_NC;
    //        goto RETURN;
    //    }
    //    
    //    if(CFIO_ID_HASH_GET_NULL == 
    //    	cfio_id_get_dim(client_nc_id, client_dim_id, &dim))
    //    {
    //        cfio_id_map_dim(client_nc_id, client_dim_id, nc->nc_id, 
    //    	    dim_id, len);
    //    }else
    //    {
    //        assert(CFIO_ID_DIM_INVALID == dim->dim_id);
    //        /**
    //         * TODO this should be a return error
    //         **/
    //        assert(dim->dim_len == len);

    //        dim->nc_id = nc->nc_id;
    //        dim->dim_id = dim_id;
    //    }

    //    _remove_client_io(io_info);
    //    debug(DEBUG_IO, "define dim(%s) success", name);
    //}

    return_code = CFIO_ERROR_NONE;

RETURN :
    return return_code;
}

int cfio_io_def_var(cfio_msg_t *msg)
{
    int ret = 0, i;
    int nc_id, var_id, ndims;
    int client_nc_id, client_var_id;
    cfio_id_nc_t *nc;
    cfio_id_dim_t **dims = NULL; 
    cfio_id_var_t *var = NULL;
    cfio_io_val_t *io_info = NULL;
    int *client_dim_ids = NULL;
    size_t *dims_len = NULL;
    char *name = NULL;
    size_t *start = NULL, *count = NULL;
    cfio_type xtype;
    int client_num;
    int client_id = msg->src;

    int func_code = FUNC_NC_DEF_VAR;
    int return_code;

    ret = cfio_recv_unpack_def_var(msg, &client_nc_id, &name, &xtype, &ndims, 
	    &client_dim_ids, &start, &count, &client_var_id);
    
#ifdef SVR_UNPACK_ONLY
    if(name != NULL)
    {
	free(name);
	name = NULL;
    }
    if(start != NULL)
    {
	free(start);
	start = NULL;
    }
    if(count != NULL)
    {
	free(count);
	count = NULL;
    }
    if(client_dim_ids != NULL)
    {
	free(client_dim_ids);
	client_dim_ids = NULL;
    }
    return CFIO_ERROR_NONE;
#endif

    if( ret < 0 )
    {
	error("unpack_msg_def_var failed");
	return CFIO_ERROR_MSG_UNPACK;
    }
    dims = malloc(ndims * sizeof(cfio_id_dim_t *));
    dims_len = malloc(ndims * sizeof(size_t));

    //_recv_client_io(
    //        client_id, func_code, client_nc_id, 0, client_var_id, &io_info);

    if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
    {
	return_code = CFIO_ERROR_INVALID_NC;
	debug(DEBUG_IO, "Invalid NC ID.");
	goto RETURN;
    }
	
    for(i = 0; i < ndims; i ++)
    {
	if(CFIO_ID_HASH_GET_NULL == cfio_id_get_dim(
		    client_nc_id, client_dim_ids[i], &dims[i]))
	{
	    debug(DEBUG_IO, "Invalid Dim.");
	    return_code = CFIO_ERROR_INVALID_DIM;
	    goto RETURN;
	}
    }

    if(CFIO_ID_HASH_GET_NULL == 
	    cfio_id_get_var(client_nc_id, client_var_id, &var))
    {
	/**
	 * the fisrst def var msg arrive, not need to free the name, start, count
	 * and client_dim_ids
	 **/
	client_num = cfio_map_get_client_num_of_server(server_id);
	cfio_id_map_var(name, client_nc_id, client_var_id, 
		CFIO_ID_NC_INVALID, CFIO_ID_VAR_INVALID, 
		ndims, client_dim_ids, start, count, xtype, client_num);
	/**
	 *set each dim's len for the var
	 **/
	for(i = 0; i < ndims; i ++)
	{
	    if((int)count[i] > (int)dims[i]->dim_len)
	    {
		dims[i]->dim_len = count[i];
	    }
	}
    }else
    {
	/**
	 *update var's start, count and each dim's len
	 **/
	for(i = 0; i < ndims; i ++)
	{
	    /**
	     *TODO handle exceed
	     **/
	    debug(DEBUG_IO, "Pre var dim %d: start(%lu), count(%lu)", 
		    i, var->start[i], var->count[i]);
	    debug(DEBUG_IO, "New var dim %d: start(%lu), count(%lu)", 
		    i, start[i], count[i]);
	}
	_update_start_and_count(ndims, var->start, var->count, start, count);
	for(i = 0; i < ndims; i ++)
	{
	    debug(DEBUG_IO, "count = %lu; dim_len = %d", 
		    var->count[i], dims[i]->dim_len);
	    if((int)(var->count[i]) > (int)(dims[i]->dim_len))
	    {
		dims[i]->dim_len = var->count[i];
	    }
	}
	for(i = 0; i < ndims; i ++)
	{
	    debug(DEBUG_IO, "Now var dim %d: start(%lu), count(%lu)", 
		    i, var->start[i], var->count[i]);
	}

	/**
	 *we need to free 4 pointer 
	 **/
	free(name);
	free(start);
	free(count);
	free(client_dim_ids);
    }
    return_code = CFIO_ERROR_NONE;

RETURN :
    if(dims != NULL)
    {
	free(dims);
	dims = NULL;
    }
    if(dims_len != NULL)
    {
	free(dims_len);
	dims_len = NULL;
    }
    return return_code;
}

int cfio_io_put_att(cfio_msg_t *msg)
{
    int client_id = msg->src;
    int client_nc_id, client_var_id; 
    int return_code, ret;
    cfio_id_nc_t *nc;
    cfio_id_var_t *var;
    cfio_io_val_t *io_info;
    char *name;
    nc_type xtype;
    int len;
    char *data;

    int func_code = FUNC_PUT_ATT;
    ret = cfio_recv_unpack_put_att(msg,
	    &client_nc_id, &client_var_id, &name, &xtype, &len, (void **)&data);
    if( ret < 0 )
    {
	error("");
	return CFIO_ERROR_MSG_UNPACK;
    }

#if defined(SVR_UNPACK_ONLY) || defined(SVR_META_ONLY)
    if(name != NULL)
    {
	free(name);
	name = NULL;
    }
    if(data != NULL)
    {
	free(data);
	data = NULL;
    }
    return CFIO_ERROR_NONE;
#endif
    
    _recv_client_io(
	    client_id, func_code, client_nc_id, 0, client_var_id, &io_info);

    if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
    {
	return_code = CFIO_ERROR_INVALID_NC;
	debug(DEBUG_IO, "Invalid NC ID.");
	goto RETURN;
    }

    if(_bitmap_full(io_info->client_bitmap))
    {
	if(client_var_id == NC_GLOBAL)
	{
	    switch(xtype)
	    {
		case CFIO_CHAR :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_text(nc->nc_id, NC_GLOBAL, name, len, data);
#else
		    ret = NC_NOERR;
#endif
		    break;
		case CFIO_INT :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_int(nc->nc_id, NC_GLOBAL, name, xtype, len, (const int*)data);
#else
		    ret = NC_NOERR;
#endif
		    break;
		case CFIO_FLOAT :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_float(nc->nc_id, NC_GLOBAL, name, xtype, len, (const float*)data);
#else
		    ret = NC_NOERR;
#endif
		    break;
		case CFIO_DOUBLE :
#ifndef SVR_NO_IO
		    ret = ncmpi_put_att_double(nc->nc_id, NC_GLOBAL, name, xtype, len, (const double*)data);
#else
		    ret = NC_NOERR;
#endif
		    break;
	    }
	    if(ret != NC_NOERR)
	    {
		error("Error happened when put attr error(%s)", 
			ncmpi_strerror(ret));
		return_code = CFIO_ERROR_NC;

		goto RETURN;
	    }
	}
	else
	{
	    if(CFIO_ID_HASH_GET_NULL == cfio_id_put_att(
			client_nc_id, client_var_id, name, xtype, len, data))
	    {
		error("");
		return_code = CFIO_ERROR_INVALID_NC;
		goto RETURN;
	    }
	}
	_remove_client_io(io_info);
    }
    return_code = CFIO_ERROR_NONE;

RETURN:
	return return_code;
    }

int cfio_io_enddef(cfio_msg_t *msg)
{
    int client_nc_id, ret;
    cfio_id_nc_t *nc;
    cfio_io_val_t *io_info;
    cfio_id_val_t *iter, *nc_val;
    int client_id = msg->src;

    int func_code = FUNC_NC_ENDDEF;

    ret = cfio_recv_unpack_enddef(msg, &client_nc_id);
    if( ret < 0 )
    {
	error("unapck msg error");
	return CFIO_ERROR_MSG_UNPACK;
    }

#ifdef SVR_UNPACK_ONLY
    return CFIO_ERROR_NONE;
#endif

    _recv_client_io(client_id, func_code, client_nc_id, 0, 0, &io_info);

    if(_bitmap_full(io_info->client_bitmap))
    {

	if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
	{
	    debug(DEBUG_IO, "Invalid NC.");
	    return CFIO_ERROR_INVALID_NC;
	}

	if(DEFINE_MODE == nc->nc_status)
	{
	    cfio_id_get_val(client_nc_id, 0, 0, &nc_val);
	    qlist_for_each_entry(iter, &(nc_val->link), link)
	    {
		if((ret = _handle_def(iter)) < 0)
		{   
		    return ret;
		}
	    }
#ifndef SVR_NO_IO
	    ret = ncmpi_enddef(nc->nc_id);
#else
	    ret = NC_NOERR;
#endif
	    if(ret < 0)
	    {
		error("enddef error(%s)",ncmpi_strerror(ret));
		return CFIO_ERROR_NC;
	    }

	    nc->nc_status = DATA_MODE;
	}
	_remove_client_io(io_info);
    }
    return CFIO_ERROR_NONE;
}

int cfio_io_put_vara(cfio_msg_t *msg)
{
    int i,ret = 0, ndims;
    cfio_id_nc_t *nc;
    cfio_id_var_t *var;
    cfio_io_val_t *io_info;
    int client_nc_id, client_var_id;
    size_t *start, *count;
    size_t *total_start = NULL, *total_count = NULL;
    size_t data_size;
    char *data;
    char *total_data = NULL;
    int data_len, data_type, client_index;
    size_t *put_start;
    int client_id = msg->src;

    int func_code = FUNC_NC_PUT_VARA;
    int return_code;

    MPI_Offset *pnc_start = NULL, *pnc_count = NULL;

    //double start_time, end_time;

    //    ret = cfio_unpack_msg_extra_data_size(h_buf, &data_size);
    ret = cfio_recv_unpack_put_vara(msg, 
	    &client_nc_id, &client_var_id, &ndims, &start, &count,
	    &data_len, &data_type, &data);	
	
    for(i = 0; i < ndims; i ++)
    {
	    debug(DEBUG_IO, "recv dim %d: start(%lu), count(%lu)", 
		    i, start[i], count[i]);
	    debug(DEBUG_IO, "recv data = %f", ((double *)data)[0]);
	//    printf( "dim %d: start(%lu), count(%lu)\n", 
	//	    i, total_start[i], total_count[i]);
    }
    debug(DEBUG_IO, "client_var_id = %d", client_var_id);
    if( ret < 0 )
    {
	error("");
	return CFIO_ERROR_MSG_UNPACK;
    }

#if defined(SVR_UNPACK_ONLY) || defined(SVR_META_ONLY)
    if(start != NULL)
    {
	free(start);
	start = NULL;
    }
    if(count != NULL)
    {
	free(count);
	count = NULL;
    }
    if(data != NULL)
    {
	free(data);
	data = NULL;
    }
    return CFIO_ERROR_NONE;
#endif

    _recv_client_io(
	    client_id, func_code, client_nc_id, 0, client_var_id, &io_info);

    client_index = cfio_map_get_client_index_of_server(client_id);
    //TODO  check whether data_type is right
    if(CFIO_ID_HASH_GET_NULL == cfio_id_put_var(
		client_nc_id, client_var_id, client_index, 
		start, count, (char*)data))
    {
	return_code = CFIO_ERROR_INVALID_VAR;
	debug(DEBUG_IO, "Invalid var.");
	goto RETURN;
    }

    if(_bitmap_full(io_info->client_bitmap))
    {
        debug(DEBUG_IO, "bit map full");

        if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc) ||
        	CFIO_ID_NC_INVALID == nc->nc_id)
        {
            return_code = CFIO_ERROR_INVALID_NC;
            debug(DEBUG_IO, "Invalid nc.");
            goto RETURN;
        }
        if(CFIO_ID_HASH_GET_NULL == 
        	cfio_id_get_var(client_nc_id, client_var_id, &var) ||
        	CFIO_ID_VAR_INVALID == var->var_id)
        {
            return_code = CFIO_ERROR_INVALID_VAR;
            debug(DEBUG_IO, "Invalid var.");
            goto RETURN;
        }

        if(ndims != var->ndims)
        {
            debug(DEBUG_IO, "wrong ndims(%s), ndims(%d), var->ndims(%d)", 
		    var->name, ndims, var->ndims);
            return_code = CFIO_ERROR_WRONG_NDIMS;
            goto RETURN;
        }

	total_start = malloc(sizeof(size_t) * var->ndims);
	total_count = malloc(sizeof(size_t) * var->ndims);
        _merge_var_data(var, total_start, total_count, &total_data);
	
	for(i = 0; i < var->ndims; i ++)
	{
	    debug(DEBUG_IO, "dim %d: start(%lu), count(%lu)", 
		    i, total_start[i], total_count[i]);
	//    printf( "server %d: dim %d: start(%lu), count(%lu)\n", 
	//	    server_id, i, total_start[i], total_count[i]);
	}
	debug(DEBUG_IO, "nc_id = %d, var_id = %d", nc->nc_id, var->var_id);
	debug(DEBUG_IO, "first data = %f", ((float *)total_data)[0]);
	
	pnc_start = malloc(sizeof(size_t) * var->ndims);
	pnc_count = malloc(sizeof(size_t) * var->ndims);
	for(i = 0; i < var->ndims; i ++)
	{
	    pnc_start[i] = total_start[i];
	    pnc_count[i] = total_count[i];
	}
	
	for(i = 0; i < var->ndims; i ++)
	{
	    debug(DEBUG_IO, "dim %d: start(%lu), count(%lu)", 
		    i, total_start[i], total_count[i]);
	    debug(DEBUG_IO, "dim %d: start(%lld), count(%lld)", 
		    i, pnc_start[i], pnc_count[i]);
	}

	switch(var->data_type)
	{
	    case CFIO_BYTE :
		break;
	    case CFIO_CHAR :
		break;
	    case CFIO_SHORT :
#ifndef SVR_NO_IO
		ret = ncmpi_put_vara_short_all(nc->nc_id, var->var_id, 
			pnc_start, pnc_count, (short*)total_data);
#else
		ret = NC_NOERR;
#endif
		break;
	    case CFIO_INT :
#ifndef SVR_NO_IO
		ret = ncmpi_put_vara_int_all(nc->nc_id, var->var_id, 
			pnc_start, pnc_count, (int*)total_data);
#else
		ret = NC_NOERR;
#endif
		break;
	    case CFIO_FLOAT :
#ifndef SVR_NO_IO
		ret = ncmpi_put_vara_float_all(nc->nc_id, var->var_id, 
			pnc_start, pnc_count, (float*)total_data);
#else
		ret = NC_NOERR;
#endif
		break;
	    case CFIO_DOUBLE :
#ifndef SVR_NO_IO
		ret = ncmpi_put_vara_double_all(nc->nc_id, var->var_id, 
			pnc_start, pnc_count, (double*)total_data);
#else
		ret = NC_NOERR;
#endif
		break;
	}
	//end_time = times_cur();
	//write_time += end_time - start_time;

        if( ret != NC_NOERR )
        {
            error("write nc(%d) var (%d) failure(%s)",
        	    nc->nc_id,var->var_id,ncmpi_strerror(ret));
            return_code = CFIO_ERROR_NC;
        }
        _remove_client_io(io_info);
    }

    return_code = CFIO_ERROR_NONE;	
    //printf("proc : %d, write_time : %f\n", server_id, write_time);

RETURN :

    if(total_data != NULL)
    {
	free(total_data);
	total_data = NULL;
    }
    if(total_count != NULL)
    {
	free(total_count);
	total_count = NULL;
    }
    if(total_start != NULL)
    {
	free(total_start);
	total_start = NULL;
    }
    if(pnc_count != NULL)
    {
	free(pnc_count);
	pnc_count = NULL;
    }
    if(pnc_start != NULL)
    {
	free(pnc_start);
	pnc_start = NULL;
    }
    return return_code;


}

int cfio_io_close(cfio_msg_t *msg)
{
    int client_nc_id, nc_id, ret;
    cfio_id_nc_t *nc;
    cfio_io_val_t *io_info;
    int func_code = FUNC_NC_CLOSE;
    cfio_id_val_t *iter, *next, *nc_val;
    int client_id = msg->src;

    ret = cfio_recv_unpack_close(msg, &client_nc_id);

    if( ret < 0 )
    {
	error("unpack close error\n");
	return ret;
    }

#ifdef SVR_UNPACK_ONLY
    return CFIO_ERROR_NONE;
#endif

    _recv_client_io(client_id, func_code, client_nc_id, 0, 0, &io_info);


    if(_bitmap_full(io_info->client_bitmap))
    {
	/*TODO handle memory free*/

	if(CFIO_ID_HASH_GET_NULL == cfio_id_get_nc(client_nc_id, &nc))
	{
	    debug(DEBUG_IO, "Invalid NC.");
	    return CFIO_ERROR_INVALID_NC;
	}
#ifndef SVR_NO_IO
	ret = ncmpi_close(nc->nc_id);
#else
	ret = NC_NOERR;
#endif

	if( ret != NC_NOERR )
	{
	    error("close nc(%d) file failure,%s\n",nc->nc_id,ncmpi_strerror(ret));
	    return CFIO_ERROR_NC;
	}
	_remove_client_io(io_info);
	
	cfio_id_get_val(client_nc_id, 0, 0, &nc_val);
	qlist_for_each_entry_safe(iter, next, &(nc_val->link), link)
	{
	    qlist_del(&(iter->link));
	    qhash_del(&(iter->hash_link));
	    cfio_id_val_free(iter);
	}
	qhash_del(&(nc_val->hash_link));
	free(nc_val);
    }
    debug(DEBUG_IO, "success return.");
    return CFIO_ERROR_NONE;
}

