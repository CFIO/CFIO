/****************************************************************************
 *       Filename:  msg.h
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
#ifndef _MSG_H
#define _MSG_H
#include <stdlib.h>

#include "mpi.h"
#include "buffer.h"
#include "quicklist.h"

/**
 *define for nc function code
 **/
#define FUNC_NC_CREATE		((uint32_t)1)
#define FUNC_NC_ENDDEF		((uint32_t)2)
#define FUNC_NC_CLOSE		((uint32_t)3)
#define FUNC_NC_DEF_DIM		((uint32_t)11)
#define FUNC_NC_DEF_VAR		((uint32_t)12)
#define FUNC_PUT_ATT		((uint32_t)13)
#define FUNC_NC_PUT_VARA	((uint32_t)20)
#define FUNC_IO_END		((uint32_t)30)
#define FUNC_FINAL		((uint32_t)40)
/* below two are only used in io.c */
#define FUNC_READER_FINAL		((uint32_t)41)
#define FUNC_WRITER_FINAL		((uint32_t)42)

//define for msg buf size in a proc
#define MSG_BUF_SIZE ((size_t)512*1024)

typedef struct
{
    uint32_t func_code;	/* function code , like FUNC_NC_CREATE */
    size_t size;	/* size of the msg */
    char *addr;		/* pointer to the data buffer of the msg */   
    int src;		/* id of sending msg proc */
    int dst;		/* id of dst porc */  
    MPI_Comm comm;	/* communication  */
    MPI_Request req;	/* MPI request of the send msg */
    qlist_head_t link;	/* quicklist head */
}cfio_msg_t;

cfio_msg_t *cfio_msg_create();

int cfio_msg_get_max_size(int proc_id);
#endif
