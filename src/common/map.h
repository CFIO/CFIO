/****************************************************************************
 *       Filename:  map.h
 *
 *    Description:  map from IO proc to IO Forwarding Proc
 *
 *        Version:  1.0
 *        Created:  12/14/2011 02:21:45 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _MAP_H
#define _MAP_H
#include "msg.h"

#define GEN_SERVER_ERROR	0.4

#define CFIO_MAP_TYPE_CLIENT	1
#define CFIO_MAP_TYPE_SERVER	2
#define CFIO_MAP_TYPE_BLANK	3 /* the proc who do nothing, because someon may 
				     start more proc than needed*/
/**
 * @brief: cfio map var init, only be called in cfio_init adn cfio_server's main
 *	function
 *
 * @param _client_x_num: client proc num of x axis 
 * @param _client_y_num: client proc num of y axis
 * @param _server_amount: server proc num
 * @param best_server_amount: best server proc num, = client_amount * SERVER_RATIO
 * @param _comm: 
 *
 * @return: error cod
 */
int cfio_map_init(
	int _client_x_num, int _client_y_num,
	int _server_amount, int best_server_amount,
	MPI_Comm _comm, MPI_Comm server_comm);
/**
 * @brief: cfio map finalize
 *
 * @return: 
 */
int cfio_map_final();
/**
 * @brief: determine a proc's type 
 *
 * @param proc_id: proc id
 *
 * @return: CFIO_MAP_TYPE_SERVER, CFIO_MAP_TYPE_CLIENT or CFIO_MAP_TYPE_BLANK
 */
int cfio_map_proc_type(int porc_id);
/**
 * @brief: get MPI communication
 *
 * @return: MPI Communication
 */
int cfio_map_get_comm();
int cfio_map_get_server_comm();
/**
 * @brief: get server proc amount
 *
 * @return: server amount
 */
int cfio_map_get_server_amount();
/**
 * @brief: get client amount
 *
 * @return: client amount
 */
int cfio_map_get_client_amount();
/**
 * @brief: get all clients id of a server
 *
 * @param server_id: server id
 * @param client_id: pointer to the array storing clients id
 *
 * @return: error code
 */
int cfio_map_get_clients(int server_id, int* client_id);
/**
 * @brief: get client number of a server
 *
 * @param server_id: the server's id
 *
 * @return: client num
 */
int cfio_map_get_client_num_of_server(int server_id);
/**
 * @brief: get server index
 *
 * @param server_id: server proc id
 *
 * @return: server index
 */
int cfio_map_get_server_index(int server_id);
/**
 * @brief: get client index in a server
 *
 * @param client_id: the client's id
 *
 * @return: client_index
 */
int cfio_map_get_client_index_of_server(int client_id);
int cfio_map_get_server_of_client(int client_id);
/**
 * @brief: map from client proc to server proc, store map information in msg struct 
 *
 * @param msg: pointer to msg struct
 *
 * @return: error code
 */
int cfio_map_forwarding(
	cfio_msg_t *msg);
/**
 * @brief: check whether a server's bitmap is full
 *
 * @param server_id: server's id
 * @param bitmap: server's' bitmap
 * @param is_full: 
 *
 * @return: error code
 */
//int cfio_map_is_bitmap_full(
//	int server_id, uint8_t *bitmap, int *is_full);

#endif
