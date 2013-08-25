/****************************************************************************
 *       Filename:  cfio_error.h
 *
 *    Description:  macro define for errors
 *
 *        Version:  1.0
 *        Created:  10/22/2012 03:15:53 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _CFIO_ERROR_H
#define _CFIO_ERROR_H

#define CFIO_ERROR_NONE               0

#define CFIO_ERROR_MALLOC	    -1
#define CFIO_ERROR_HASH_TABLE_INIT  -2	    /* data index exceeds dimension bound */
#define CFIO_ERROR_INVALID_INIT_ARG -3
#define CFIO_ERROR_ARG_NULL	    -4	    /* some arg is NULL */
/* In server.c */
#define CFIO_ERROR_PTHREAD_CREATE   -100
#define CFIO_ERROR_UNEXPECTED_MSG   -101
/* In cfio.c */
#define CFIO_ERROR_FINAL_AFTER_MPI  -200    /* cfio_final should be called before
					       mpi_final*/
#define CFIO_ERROR_RANK_INVALID	    -201    
/* In msg.c */
#define CFIO_ERROR_MPI_RECV	    -300    /* MPI_Recv error */
/* In id.c */
#define CFIO_ERROR_EXCEED_BOUND	    -400    /* data index exceeds dimension bound */
#define CFIO_ERROR_NC_NO_EXIST	    -401    /* nc_id not found in assign_table */
#define CFIO_ERROR_VAR_NO_EXIST	    -401    /* nc_id not found in assign_table */
/* In io.c */
#define CFIO_ERROR_NC		    -500    /* nc operation error */
#define CFIO_ERROR_INVALID_NC	    -501    /* invalid nc id */
#define CFIO_ERROR_INVALID_DIM	    -502    /* invalid dimension id */
#define CFIO_ERROR_INVALID_VAR	    -503    /* invalid variable id */
#define CFIO_ERROR_MSG_UNPACK	    -504
#define CFIO_ERROR_PUT_VAR	    -505
#define CFIO_ERROR_WRONG_NDIMS	    -506    /* Wrong ndims in put var */
#define CFIO_ERROR_NC_NOT_DEFINE    -507    /* nc file is not in DEFINE_MODE, some
					       IO function only can be called in 
					       DEFINE_MODE */

#endif
