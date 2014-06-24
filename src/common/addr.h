#ifndef _ADDR_H 
#define _ADDR_H 

#include <stdio.h>
#include "buffer.h"

typedef struct {
    int signal; 
    // for server:
    //	0: state of init or clear 
    //	1: addr was writen by client, so to read data, io
    //	11: addr was writen by client, means the IO end.
    // for client:
    //	0: addr was writen by server, so to write new addr to server
    //	1: init & clear
    //	11: end 
    //	10: addr was writen by server, means the IO end.

    char *start_addr;
    char *end_addr;
    char *free_addr;
    char *used_addr;
} cfio_buf_addr_t;

/**
 * assign addr
 */
inline void cfio_rdma_addr_init(cfio_buf_addr_t *addr, cfio_buf_t *buf);

/**
 * server wait until addr being writen
 */
inline void cfio_rdma_addr_server_wait(
	volatile cfio_buf_addr_t *addr);

/**
 * client wait until addr being writen
 */
inline void cfio_rdma_addr_client_wait(
	volatile cfio_buf_addr_t *addr);

/**
 * client wait until until recved end signal 10
 */
inline void cfio_rdma_addr_client_wait_end(
	volatile cfio_buf_addr_t *addr);

/**
 * server test whether a addr has being writen
 * return:  0: not writen,
 *	    1: writen, client has data to transfer
 *	    11: IO end.
 */
inline int cfio_rdma_addr_server_test(
	volatile cfio_buf_addr_t *addr);

/**
 * client test whether addr has being writen
 * return:  1: not writen,
 *	    0: writen, server has writen back
 */
inline int cfio_rdma_addr_client_test(
	volatile cfio_buf_addr_t *addr);

/**
 * server clear addr's signal 
 */
inline void cfio_rdma_addr_server_clear_signal(
	volatile cfio_buf_addr_t *addr);

/**
 * client clear addr's signal 
 */
inline void cfio_rdma_addr_client_clear_signal(
	volatile cfio_buf_addr_t *addr);

/**
 * server set IO end signal 
 */
inline void cfio_rdma_addr_server_end_signal(
	volatile cfio_buf_addr_t *addr);

/**
 * client set IO end signal 
 */
inline void cfio_rdma_addr_client_end_signal(
	volatile cfio_buf_addr_t *addr);

/**
 * server show addr 
 */
void cfio_rdma_addr_server_show(
	int rank,
	int peer,
	volatile cfio_buf_addr_t *addr, 
	int itr);

/**
 * client show addr 
 */
void cfio_rdma_addr_client_show(
	int rank,
	int peer,
	volatile cfio_buf_addr_t *addr,
	int itr);

#endif
