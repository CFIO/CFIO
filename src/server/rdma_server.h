#ifndef _CFIO_RDMA_SERVER_H
#define _CFIO_RDMA_SERVER_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <mpi.h>

/**
 * connect to all mapped client, register rdma mem, recv and send rdma mem.
 */
void cfio_rdma_server_init(
	int region_amt, // mapped client amount
	int data_region_size, // recv buf's size
	char **data_regions, 
	int addr_region_size, // sizeof(cfio_buf_addr_t)
	char **addr_regions); // &cfio_buf_addr_t

/**
 * disconnect, deregister
 */
void cfio_rdma_server_final();

/**
 * read data from client's rdma buffer
 */
void cfio_rdma_server_read_data(
	int client_index, // client index of server
	int remote_offset,
	int length,
	int local_offset);

/**
 * get server's data region about client_index
 */
inline char * cfio_rdma_server_get_data(
	int client_index);

/**
 * show server's data region about client_index
 */
inline void cfio_rdma_server_show_data(
	int client_index);

/**
 * write cfio_buf_addr_t to client
 */
void cfio_rdma_server_write_addr(
	int client_index);

/**
 * send ack (cfio_buf_addr_t) to client
 */
void cfio_rdma_server_send_ack(
	int client_index);

/**
 * post request to rdma recv cfio_buf_addr_t from client
 */
void cfio_rdma_server_recv_addr(
	int client_index);

/**
 * get cfio_buf_addr_t about client_index
 */
inline char * cfio_rdma_server_get_addr(
	int client_index);

/**
 * wait all posted request for achievement.
 */
inline void cfio_rdma_server_wait_all();

/**
 * wait some posted request for achievement.
 */
int cfio_rdma_server_wait_some();

/**
 * wait completion for one poll_cq if posted request .
 */
inline void cfio_rdma_server_wait_one();

/**
 * test posted request for achievement.
 */
inline int cfio_rdma_server_test(
	int client_index);

/**
 * test posted request for achievement.
 */
inline int cfio_rdma_server_test_show(
	int client_index);

#endif
