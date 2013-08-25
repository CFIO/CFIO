/*
 * =====================================================================================
 *
 *       Filename:  cfio.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/19/2011 06:44:56 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  drealdal (zhumeiqi), meiqizhu@gmail.com
 *        Company:  Tsinghua University, HPC
 *
 * =====================================================================================
 */
#include <assert.h>
#include <pthread.h>

#include "mpi.h"

#include "cfio.h"
#include "send.h"
#include "map.h"
#include "id.h"
#include "buffer.h"
#include "debug.h"
#include "times.h"
#include "cfio_error.h"

/* my real rank in mpi_comm_world */
static int rank;
/*  the num of the app proc*/
static int client_num;
static MPI_Comm inter_comm;
static MPI_Comm client_comm, server_comm;

int cfio_init(int x_proc_num, int y_proc_num, int ratio)
{
    int rc, i;
    int size;
    int root = 0;
    int error, ret;
    int server_proc_num;
    int best_server_amount;
    MPI_Group group, client_group, server_group;
    int *ranks;

    //set_debug_mask(DEBUG_CFIO | DEBUG_SERVER);// | DEBUG_MSG | DEBUG_SERVER);
    rc = MPI_Initialized(&i); 
    if( !i )
    {
	error("MPI should be initialized before the cfio\n");
	return -1;
    }

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //if(rank == 100)
    //{
    //    set_debug_mask(DEBUG_MAP);
    //}
    
    client_num = x_proc_num * y_proc_num;
    server_proc_num = size - client_num;
    if(server_proc_num < 0)
    {
	server_proc_num = 0;
    }
    
    best_server_amount = (int)((double)client_num / ratio);
    if(best_server_amount <= 0)
    {
	best_server_amount = 1;
    }

    MPI_Comm_group(MPI_COMM_WORLD, &group);
    
    //ranks = malloc(client_num * sizeof(int));
    //for(i = 0; i < client_num; i ++)
    //{
    //    ranks[i] = i;
    //}
    //MPI_Group_incl(group, client_num, ranks, &client_group);
    //MPI_Comm_create(MPI_COMM_WORLD, client_group, &client_comm);
    //free(ranks);

    ranks = malloc(server_proc_num * sizeof(int));
    for(i = 0; i < server_proc_num; i ++)
    {
	ranks[i] = i + client_num;
    }
    MPI_Group_incl(group, server_proc_num, ranks, &server_group);
    MPI_Comm_create(MPI_COMM_WORLD, server_group, &server_comm);
    free(ranks);

    //times_start();

    if((ret = cfio_map_init(
		    x_proc_num, y_proc_num, server_proc_num, 
		    best_server_amount, MPI_COMM_WORLD, server_comm)) < 0)
    {
	error("Map Init Fail.");
	return ret;
    }

    if(cfio_map_proc_type(rank) == CFIO_MAP_TYPE_SERVER)
    {
	if((ret = cfio_server_init()) < 0)
	{
	    error("");
	    return ret;
	}
	if((ret = cfio_server_start()) < 0)
	{
	    error("");
	    return ret;
	}
    }else if(cfio_map_proc_type(rank) == CFIO_MAP_TYPE_CLIENT)
    {
	if((ret = cfio_send_init(CLIENT_BUF_SIZE)) < 0)
	{
	    error("");
	    return ret;
	}

	if((ret = cfio_id_init(CFIO_ID_INIT_CLIENT)) < 0)
	{
	    error("");
	    return ret;
	}
	
    }

    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_finalize()
{
    int ret,flag;
    cfio_msg_t *msg;

    ret = MPI_Finalized(&flag);
    if(flag)
    {
	error("***You should not call MPI_Finalize before cfio_Finalized*****\n");
	return CFIO_ERROR_FINAL_AFTER_MPI;
    }
    if(cfio_map_proc_type(rank) == CFIO_MAP_TYPE_CLIENT)
    {
	cfio_send_io_done(&msg, rank);
    }
    
    if(cfio_map_proc_type(rank) == CFIO_MAP_TYPE_SERVER)
    {
	cfio_server_final();
    }else if(cfio_map_proc_type(rank) == CFIO_MAP_TYPE_CLIENT)
    {
	cfio_id_final();

	cfio_send_final();
    }

    cfio_map_final();
    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_proc_type()
{
    int type;

    type = cfio_map_proc_type(rank);

    debug(DEBUG_CFIO, "rank(%d)'s type = %d", rank, type);

    return type;
}

/**
 * @brief: create
 *
 * @param path: the file anme of the new netCDF dataset
 * @param cmode: the creation mode flag
 * @param ncidp: pointer to location where returned netCDF ID is to be stored
 *
 * @return: 0 if success
 */
int cfio_create(
	char *path, int cmode, int *ncidp)
{
    if(path == NULL || ncidp == NULL)
    {
	error("args should not be NULL.");
	return CFIO_ERROR_ARG_NULL;
    }

    cfio_msg_t *msg;
    int ret;

    if((ret = cfio_id_assign_nc(ncidp)) < 0)
    {
	error("");
	return ret;
    }

    cfio_send_create(path, cmode, *ncidp);

    debug(DEBUG_CFIO, "path = %s, ncid = %d", path, *ncidp);

    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}
/**
 * @brief: def_dim
 *
 * @param ncid: NetCDF group ID
 * @param name: Dimension name
 * @param len: Length of dimension
 * @param idp: pointer to location for returned dimension ID
 *
 * @return: 0 if success
 */
int cfio_def_dim(
	int ncid, char *name, size_t len, int *idp)
{
    if(name == NULL || idp == NULL)
    {
	error("args should not be NULL.");
	return CFIO_ERROR_ARG_NULL;
    }
    
    int ret;

    debug(DEBUG_CFIO, "ncid = %d, name = %s, len = %lu",
	    ncid, name, len);
    
    cfio_msg_t *msg;

    if((ret = cfio_id_assign_dim(ncid, name, idp)) < 0)
    {
	error("");
	return ret;
    }

    cfio_send_def_dim(ncid, name, len, *idp);

    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_def_var(
	int ncid, char *name, cfio_type xtype,
	int ndims, int *dimids, 
	size_t *start, size_t *count, 
	int *varidp)
{
    if(name == NULL || varidp == NULL || start == NULL || count == NULL)
    {
	error("args should not be NULL.");
	return CFIO_ERROR_ARG_NULL;
    }
    
    debug(DEBUG_CFIO, "ndims = %d", ndims);

    cfio_msg_t *msg;
    int ret;

    if((ret = cfio_id_assign_var(ncid, name, varidp)) < 0)
    {
	error("");
	return ret;
    }
    
    cfio_send_def_var(ncid, name, xtype, 
	    ndims, dimids, start, count, *varidp);
    
    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_put_att(
	int ncid, int varid, char *name, 
	cfio_type xtype, size_t len, void *op)
{
    cfio_msg_t *msg;
    
    //if(cfio_map_get_client_index_of_server(rank) == 0)
    //{
    cfio_send_put_att(ncid, varid, name,
	    xtype, len, op);
    //}
    debug(DEBUG_CFIO, "ncid = %d, var_id = %d, name = %s, len = %lu",
	    ncid, varid, name, len);

    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_enddef(
	int ncid)
{
    cfio_msg_t *msg;

    cfio_send_enddef(ncid);

    debug(DEBUG_CFIO, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_inq_varid(int ncid, char *var_name, int *varid)
{
    int ret;

    debug(DEBUG_CFIO, "nc_id = %d, var_name = %s", ncid, var_name);
    
    if((ret = cfio_id_inq_var(ncid, var_name, varid)) < 0)
    {
	error("");
	return ret;
    }

    debug(DEBUG_CFIO, "get varid = %d", *varid);
    return CFIO_ERROR_NONE;

}

int cfio_put_vara_float(
	int ncid, int varid, int dim,
	size_t *start, size_t *count, float *fp)
{
    if(start == NULL || count == NULL || fp == NULL)
    {
	error("args should not be NULL.");
	return CFIO_ERROR_ARG_NULL;
    }

    cfio_msg_t *msg;

    //times_start();

    //head_size = 6 * sizeof(int) + 2 * dim * sizeof(size_t);

    //_put_vara(io_proc_id, ncid, varid, dim,
    //  start, count, CFIO_FLOAT, fp, head_size, dim - 1);
    debug(DEBUG_CFIO, "start :(%lu, %lu), count :(%lu, %lu)", 
	    start[0], start[1], count[0], count[1]);
    cfio_send_put_vara(ncid, varid, dim, 
	    start, count, CFIO_FLOAT, fp);

    debug_mark(DEBUG_CFIO);

	//debug(DEBUG_TIME, "%f ms", times_end());

    return CFIO_ERROR_NONE;
}

int cfio_put_vara_double(
	int ncid, int varid, int dim,
	size_t *start, size_t *count, double *fp)
{
    if(start == NULL || count == NULL || fp == NULL)
    {
	error("args should not be NULL.");
	return CFIO_ERROR_ARG_NULL;
    }

    cfio_msg_t *msg;

	//times_start();
    debug(DEBUG_CFIO, "start :(%lu, %lu), count :(%lu, %lu)", 
	    start[0], start[1], count[0], count[1]);

    //head_size = 6 * sizeof(int) + 2 * dim * sizeof(size_t);

    //_put_vara(io_proc_id, ncid, varid, dim,
    //        start, count, CFIO_DOUBLE, fp, head_size, dim - 1);
    cfio_send_put_vara(ncid, varid, dim, 
	    start, count, CFIO_DOUBLE, fp);

    debug_mark(DEBUG_CFIO);

	//debug(DEBUG_TIME, "%f ms", times_end());

    return CFIO_ERROR_NONE;
}

int cfio_put_vara_int(
	int ncid, int varid, int dim,
	size_t *start, size_t *count, int *fp)
{
    if(start == NULL || count == NULL || fp == NULL)
    {
	error("args should not be NULL.");
	return CFIO_ERROR_ARG_NULL;
    }

    cfio_msg_t *msg;

	//times_start();
    debug(DEBUG_CFIO, "start :(%lu, %lu), count :(%lu, %lu)", 
	    start[0], start[1], count[0], count[1]);

    //head_size = 6 * sizeof(int) + 2 * dim * sizeof(size_t);

    //_put_vara(io_proc_id, ncid, varid, dim,
    //        start, count, CFIO_DOUBLE, fp, head_size, dim - 1);
    cfio_send_put_vara(ncid, varid, dim, 
	    start, count, CFIO_INT, fp);

    debug_mark(DEBUG_CFIO);

	//debug(DEBUG_TIME, "%f ms", times_end());

    return CFIO_ERROR_NONE;
}

int cfio_io_end()
{
    debug(DEBUG_CFIO, "Start cfio_io_end");


    
    cfio_send_io_end();

#ifndef async_send
    //MPI_Barrier(client_comm);
#endif

#ifdef async_isend
    cfio_send_test();
#endif

    debug(DEBUG_CFIO, "Finish cfio_io_end");
    return CFIO_ERROR_NONE;
}

int cfio_close(
	int ncid)
{

    cfio_msg_t *msg;
    int ret;
    //times_start();

    if((ret = cfio_id_remove_nc(ncid)) < 0)
    {
	error("");
	return ret;
    }
    cfio_send_close(ncid);

    //cfio_msg_test();

    debug(DEBUG_CFIO, "Finish cfio_close");

    return CFIO_ERROR_NONE;
}

/**
 *For Fortran Call
 **/
void cfio_init_c_(int *x_proc_num, int *y_proc_num, int *ratio, int *ierr)
{
    *ierr = cfio_init(*x_proc_num, *y_proc_num, *ratio);
}

void cfio_finalize_c_(int *ierr)
{
    *ierr = cfio_finalize();
}

void cfio_proc_type_c_(int *type)
{
    *type = cfio_proc_type();
}

void cfio_create_c_(
	char *path, int *len, int *cmode, int *ncidp, int *ierr)
{
    debug(DEBUG_CFIO, "path = %s, cmode = %d", path, *cmode);

    path[*len] = 0;

    *ierr = cfio_create(path, *cmode, ncidp);

    return;
}
void cfio_def_dim_c_(
	int *ncid, char *name, int *name_len, int *len, int *idp, int *ierr)
{
    size_t _len;

    _len = (int)(*len);

    name[*name_len] = 0;

    *ierr = cfio_def_dim(*ncid, name, _len, idp);

    return;
}

void cfio_def_var_c_(
	int *ncid, char *name, int *name_len,
	cfio_type *xtype, int *ndims, int *dimids, 
	int *start, int *count, int *varidp,
	int *ierr)
{
    size_t *_start, *_count;
    int *_dimids;
    int i, j;

    _start = malloc((*ndims) * sizeof(size_t));
    if(NULL == _start)
    {
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    _count = malloc((*ndims) * sizeof(size_t));
    if(NULL == _count)
    {
	free(_start);
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    _dimids = malloc((*ndims) * sizeof(int));
    if(NULL == _count)
    {
	free(_start);
	free(_count);
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    for(i = 0, j = (*ndims) - 1; i < (*ndims); i ++, j --)
    {
	_start[i] = start[j] - 1;
	_count[i] = count[j];
	_dimids[i] = dimids[j];
    }

    name[*name_len] = 0;

    *ierr = cfio_def_var(*ncid, name, *xtype, *ndims, _dimids, 
	    _start, _count, varidp);
    
    free(_start);
    free(_count);
    free(_dimids);
    return;
}

void cfio_put_att_c_(
	int *ncid, int *varid, char *name, int *name_len,
	cfio_type *xtype, int *len, void *op, int *ierr)
{
    int _varid;

    /* NF90_GLOBAL = 0 but NC_GLOBAL = -1 */
    if(*varid == 0)
    {
	_varid = -1;
    }else
    {
	_varid = *varid;
    }
    name[*name_len] = 0;

    *ierr = cfio_put_att(*ncid, _varid, name, *xtype, *len, op);

    return;
}
void cfio_put_vara_float_c_(
	int *ncid, int *varid, int *ndims,
	int *start, int *count, float *fp, int *ierr)
{
    size_t *_start, *_count;
    int i, j;

    debug(DEBUG_CFIO, "ndims : %d, start :(%d, %d), count :(%d, %d)", 
	    *ndims, start[0], start[1], count[0], count[1]);
    
    _start = malloc((*ndims) * sizeof(size_t));
    if(NULL == _start)
    {
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    _count = malloc((*ndims) * sizeof(size_t));
    if(NULL == _count)
    {
	free(_start);
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    for(i = 0, j = (*ndims) - 1; i < (*ndims); i ++, j --)
    {
	_start[i] = start[j] - 1;
	_count[i] = count[j];
    }
    *ierr = cfio_put_vara_float(
	    *ncid, *varid, *ndims, _start, _count, fp);
    
    free(_start);
    free(_count);
    return;
}

void cfio_put_vara_double_c_(
	int *ncid, int *varid, int *ndims,
	int *start, int *count, double *fp, int *ierr)
{
    size_t *_start, *_count;
    int i, j;

    _start = malloc((*ndims) * sizeof(size_t));
    if(NULL == _start)
    {
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    _count = malloc((*ndims) * sizeof(size_t));
    if(NULL == _count)
    {
	free(_start);
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    for(i = 0, j = (*ndims) - 1; i < (*ndims); i ++, j --)
    {
	_start[i] = start[j] - 1;
	_count[i] = count[j];
    }
    *ierr = cfio_put_vara_double(
	    *ncid, *varid, *ndims, _start, _count, fp);
    
    free(_start);
    free(_count);
    return;
}

void cfio_put_vara_int_c_(
	int *ncid, int *varid, int *ndims,
	int *start, int *count, int *fp, int *ierr)
{
    size_t *_start, *_count;
    int i, j;

    _start = malloc((*ndims) * sizeof(size_t));
    if(NULL == _start)
    {
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    _count = malloc((*ndims) * sizeof(size_t));
    if(NULL == _count)
    {
	free(_start);
	debug(DEBUG_CFIO, "malloc fail");
	*ierr = CFIO_ERROR_MALLOC;
	return;
    }
    for(i = 0, j = (*ndims) - 1; i < (*ndims); i ++, j --)
    {
	_start[i] = start[j] - 1;
	_count[i] = count[j];
    }
    *ierr = cfio_put_vara_int(
	    *ncid, *varid, *ndims, _start, _count, fp);
    
    free(_start);
    free(_count);
    return;
}

void cfio_enddef_c_(
	int *ncid, int *ierr)
{
    *ierr = cfio_enddef(*ncid);
    return;
}

void cfio_inq_varid_c_(int *ncid, char *var_name, int *name_len, int *varid, int *ierr)
{
    char *name;
    
    name = malloc(*name_len + 1);
    snprintf(name, (*name_len + 1), "%s", var_name);
    

    *ierr = cfio_inq_varid(*ncid, name, varid);

    free(name);

    return;
}

void cfio_close_c_(
	int *ncid, int *ierr)
{
    *ierr = cfio_close(*ncid);
    return;
}

void cfio_io_end_c_(int *ierr)
{
    *ierr = cfio_io_end();

    return;
}

