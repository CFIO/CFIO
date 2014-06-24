#ifndef _CFIO_RDMA_CLIENT_H
#define _CFIO_RDMA_CLIENT_H

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
 * connect to server, 
 * register local data and addr memory, 
 * receive server's rdma data and addr memory
 */
void cfio_rdma_client_init(
	int data_region_size, // cfio_buf_t->size
	char *data_region, // cfio_buf_t->start
	int addr_region_size, // sizeof(cfio_buf_addr_t)
	char *addr_region); // &cfio_buf_addr_t

/**
 * disconect, deregister
 */
void cfio_rdma_client_final();

/**
 * client rdma write data to server.
 */
void cfio_rdma_client_write_data(
	int remote_offset, 
	int length, 
	int local_offset);

/**
 * show client data region
 */
inline void cfio_rdma_client_show_data();

/**
 * client rdma write cfio_buf_addr_t to server.
 */
void cfio_rdma_client_write_addr();

/**
 * client rdma send cfio_buf_addr_t to server.
 */
void cfio_rdma_client_send_addr();

/**
 * client post request to rdma recv ack (cfio_buf_addr_t) from server.
 */
void cfio_rdma_client_recv_ack();

/**
 * resume poll_cq thread
 */
inline void cfio_rdma_client_poll_resume();

/**
 * pause poll_cq thread.
 * It's efficient to pause the poll_cq thread when no I/O request.
 */
inline void cfio_rdma_client_poll_pause();

/**
 * client wait all posted rdma request for achievement
 */
inline void cfio_rdma_client_wait();

/**
 * client wait all posted rdma request for achievement
 */
inline void cfio_rdma_client_wait_temp();

#endif
