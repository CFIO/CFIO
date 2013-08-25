/****************************************************************************
 *       Filename:  recv.h
 *
 *    Description:  define for msg between IO process and IO forwarding process
 *
 *        Version:  1.0
 *        Created:  12/13/2011 10:39:50 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _RECV_H
#define _RECV_H
#include <stdlib.h>

#include "msg.h"
#include "cfio_types.h"

#define RECV_BUF_SIZE ((size_t)1*1024*1024*1024)

#define CFIO_RECV_BUF_FULL 1

/**
 * @brief: init the buffer and msg queue
 *
 * @return: error code
 */
int cfio_recv_init();
/**
 * @brief: finalize , free the buffer and msg queue
 *
 * @return: error code
 */
int cfio_recv_final();
int cfio_iprobe(
	int *src, int src_len, MPI_Comm comm, int *flag);
/**
 * @brief: recv msg from client
 *
 * @param rank: the rank of server who recv the msg
 * @param comm: MPI communicator
 * @param _msg: point to the msg which is recieved
 *
 * @return: error code
 */
int cfio_recv(
	int src, int rank, MPI_Comm comm, uint32_t *func_code);

/**
 * @brief: get the first msg in msg queue
 *
 * @return: pointer to the first msg
 */
cfio_msg_t* cfio_recv_get_first();
int cfio_recv_unpack_msg_size(cfio_msg_t *msg, size_t *size);
/**
 * @brief: unpack funciton code from the buffer
 *
 * @param msg: pointer to the recv msg
 * @param func_code: the code of fucntion
 *
 * @return: error code
 */
int cfio_recv_unpack_func_code(
	cfio_msg_t *msg,
	uint32_t *func_code);
/**
 * @brief: unpack arguments for ifow_create function
 *
 * @param path: poiter to where the file anme of the new netCDF dataset is to be 
 *	stored
 * @param cmode: pointer to where the creation mode flag is to be stored
 * @param ncid: pointer to where the ncid assigned by client is to be stored
 *
 * @return: error code
 */
int cfio_recv_unpack_create(
	cfio_msg_t *msg,
	char **path, int *cmode, int *ncid);
/**
 * @brief: unpack arguments for ifow_def_dim function
 *
 * @param ncid: pointer to where NetCDF group ID is to be stored
 * @param name: pointer to where dimension name is to be stored
 * @param len: pointer to where length of dimension is to be stored
 * @param dimid: pointer to where the dimid assigned by client is to be stored
 *
 * @return: error code
 */
int cfio_recv_unpack_def_dim(
	cfio_msg_t *msg,
	int *ncid, char **name, size_t *len,int *dimid);
/**
 * @brief: unpack arguments for the cfio_def_var function
 *
 * @param ncid: pointer to where netCDF ID is to be stored
 * @param name: pointer to where variable name is to be stored, need to be freed by
 *	the caller
 * @param xtype: pointer to where netCDF external data types is to be stored
 * @param ndims: pointer to where number of dimensions for the variable is to be 
 *	stored
 * @param dimids: pointer to where vector of ndims dimension IDs corresponding to
 *      the variable dimensions is to be stored, need to be freed by the caller
 * @param start: pointer to where the start index of to be written data value 
 *	to be stored, need to be freed by the caller
 * @param count: pointer to where the size of to be written data dimension len
 *	value to be stored, need to be freed by the caller
 * @param varid: pointer to where the varid assigned by client is to be stored
 *
 * @return: error code
 */
int cfio_recv_unpack_def_var(
	cfio_msg_t *msg,
	int *ncid, char **name, cfio_type *xtype,
	int *ndims, int **dimids, 
	size_t **start, size_t **count, int *varid);
/**
 * @brief: unpack arguments for cfio_put_att
 *
 * @param ncid: pointer to where netCDF ID is to be stored
 * @param varid: pointer to where variable ID is to be stored 
 * @param name: pointer to where variable name is to be stored, need to be freed by
 *	the caller
 * @param xtype: pointer to where netCDF external data types is to be stored
 * @param len: pointer to where number of values provided for the attribute is to be
 *	stored
 * @param op: Pointer to values
 *
 * @return: 
 */
int cfio_recv_unpack_put_att(
	cfio_msg_t *msg,
	int *ncid, int *varid, char **name, 
	cfio_type *xtype, int *len, void **op);
/**
 * @brief: unpack arguments for the cfio_enddef function 
 *
 * @param ncid: pointer to where netCDF ID is to be stored
 *
 * @return: error code
 */
int cfio_recv_unpack_enddef(
	cfio_msg_t *msg,
	int *ncid);
/**
 * @brief: unpack arguments for function : cfio_recv_unpack_put_vara_**
 *
 * @param ncid: pointer to where netCDF ID is to be stored
 * @param varid: pointer to where variable ID is to be stored 
 * @param ndims: pointer to where the dimensionality of to be written variable
 *	 is to be stored 
 * @param start: pointer to where the start index of to be written data value 
 *	to be stored
 * @param count: pointer to where the size of to be written data dimension len
 *	value to be stored
 * @param data_len: pointer to the size of data 
 * @param fp_type: pointer to type of data, can be CFIO_BYTE, CFIO_CHAR, 
 *	CFIO_SHROT, CFIO_INT, CFIO_FLOAT, CFIO_DOUBLE
 * @param fp: where the data is stored
 *
 * @return: error code
 */
int cfio_recv_unpack_put_vara(
	cfio_msg_t *msg,
	int *ncid, int *varid, int *ndims, 
	size_t **start, size_t **count,
	int *data_len, int *fp_type, char **fp);
/**
 * @brief: unpack arguments for the cfio_close function
 *
 * @param ncid: pointer to where netCDF ID is to be stored
 *
 * @return: error code
 */
int cfio_recv_unpack_close(
	cfio_msg_t *msg,
	int *ncid);

#endif
