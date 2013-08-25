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
#include "debug.h"
#include "times.h"
#include "map.h"
#include "send.h"
#include "recv.h"
#include "cfio_error.h"
#include "define.h"

#define min(a,b) (a<b?a:b)
#define max(a,b) (a>b?a:b)

cfio_msg_t *cfio_msg_create()
{
    cfio_msg_t *msg;
    msg = malloc(sizeof(cfio_msg_t));
    if(NULL == msg)
    {
	return NULL;
    }
    memset(msg, 0, sizeof(cfio_msg_t));

    return msg;
}

int cfio_msg_get_max_size(int proc_id)
{   
    int client_num_of_server, max_msg_size, client_amount, server_id; 
    
    if(cfio_map_proc_type(proc_id) == CFIO_MAP_TYPE_CLIENT)
    {
	server_id = cfio_map_get_server_of_client(proc_id);
	client_num_of_server = cfio_map_get_client_num_of_server(server_id);
    }else if(cfio_map_proc_type(proc_id) == CFIO_MAP_TYPE_SERVER)
    {
	client_num_of_server = cfio_map_get_client_num_of_server(proc_id);
	
    }
    client_amount = cfio_map_get_client_amount();

    max_msg_size = MSG_BUF_SIZE;
    max_msg_size = min(max_msg_size, RECV_BUF_SIZE / client_num_of_server / 2);
    max_msg_size = min(max_msg_size, SEND_BUF_SIZE / 2);
    max_msg_size = max(max_msg_size, SEND_MSG_MIN_SIZE / client_amount);
    
    //printf("max_msg_size = %d\n", max_msg_size);

    return max_msg_size;
}
