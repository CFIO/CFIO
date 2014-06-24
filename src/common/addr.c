
#include "addr.h"

inline void cfio_rdma_addr_init(cfio_buf_addr_t *addr, cfio_buf_t *buf)
{
    addr->signal = 0;
    if (buf) {
	addr->start_addr = buf->start_addr;
	addr->free_addr = buf->free_addr;
	addr->used_addr = buf->used_addr;
	addr->end_addr = buf->end_addr;
    }
}

inline void cfio_rdma_addr_server_wait(
	volatile cfio_buf_addr_t *addr)
{
    while (!addr->signal);
}

inline void cfio_rdma_addr_client_wait(
	volatile cfio_buf_addr_t *addr)
{
    while (1 == addr->signal);
}

inline void cfio_rdma_addr_client_wait_end(
	volatile cfio_buf_addr_t *addr)
{
    // wait for 10
    while (11 == addr->signal);
}

inline int cfio_rdma_addr_server_test(
	volatile cfio_buf_addr_t *addr)
{
    return addr->signal;
}

inline int cfio_rdma_addr_client_test(
	volatile cfio_buf_addr_t *addr)
{
    return (addr->signal)? 0: 1;
}

inline void cfio_rdma_addr_server_clear_signal(
	volatile cfio_buf_addr_t *addr)
{
    addr->signal = 0;
}

inline void cfio_rdma_addr_client_clear_signal(
	volatile cfio_buf_addr_t *addr)
{
    addr->signal = 1;
}

inline void cfio_rdma_addr_server_end_signal(
	volatile cfio_buf_addr_t *addr)
{
    addr->signal = 10;
}

inline void cfio_rdma_addr_client_end_signal(
	volatile cfio_buf_addr_t *addr)
{
    addr->signal = 11;
}

void cfio_rdma_addr_server_show(
	int rank,
	int peer,
	volatile cfio_buf_addr_t *addr,
	int itr)
{
	printf("itr %d server %d client %d WT addr, signal, used, free, end: %d, %lu, %lu, %lu. \n", itr, rank, peer,   
		addr->signal, (uintptr_t)addr->used_addr, (uintptr_t)addr->free_addr, (uintptr_t)addr->end_addr);
}

void cfio_rdma_addr_client_show(
	int rank,
	int peer,
	volatile cfio_buf_addr_t *addr,
	int itr)
{
	printf("itr %d client %d server %d WR addr, signal, used, free, end: %d, %lu, %lu, %lu. \n", itr, rank, peer,  
		addr->signal, (uintptr_t)addr->used_addr, (uintptr_t)addr->free_addr, (uintptr_t)addr->end_addr);
}

