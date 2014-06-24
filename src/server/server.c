/****************************************************************************
 *       Filename:  server.c
 *
 *    Description:  main program for server
 *
 *        Version:  1.0
 *        Created:  03/13/2012 02:42:11 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <pthread.h>
#include <string.h>

#include "server.h"
#include "recv.h"
#include "io.h"
#include "id.h"
#include "mpi.h"
#include "debug.h"
#include "times.h"
#include "define.h"
#include "cfio_error.h"
#include "addr.h"
#include "rdma_server.h"

#define rdma_buf_exist_msg() ((copy_lens[clt] + sizeof(size_t) <= local_lens[clt])? 1: 0)
#define rdma_buf_empty() ((local_lens[clt] > 0)? 0: 1)
#define rdma_buf_full() ((local_lens[clt] < SERVER_REGION_SIZE)? 0: 1)

/* my real rank in mpi_comm_world */
static int rank;

static int client_num;
static int *client_ids;

static char **recv_bufs;
static int SERVER_REGION_SIZE;
static cfio_buf_addr_t **buf_addrs;
static char **read_addrs; // start addr of buffer to be read

static int *read_dones;
static int *local_lens; // length of data region that does not be copied
static int *copy_lens; // length of data region has been copied

static int *msg_cnts;
static int msg_clt_cnt = 0;

static int *comm_dones;
static int comm_done_cnt = 0;

static int *itrs;
static int low_itr;
static int low_itr_clt_cnt;

static pthread_mutex_t io_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t io_cond = PTHREAD_COND_INITIALIZER;
static volatile int paused = 1;

static double init_time, begin_time, end_time;

static int decode(cfio_msg_t *msg)
{	
    int client_id = 0;
    int ret = 0;
    uint32_t code;
    cfio_recv_unpack_func_code(msg, &code);
    client_id = msg->src;

    switch(code)
    {
	case FUNC_NC_CREATE: 
	    debug(DEBUG_SERVER,"server %d recv nc_create from client %d",
		    rank, client_id);
	    cfio_io_create(msg);
	    debug(DEBUG_SERVER, "server %d done nc_create for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_DEF_DIM:
	    debug(DEBUG_SERVER,"server %d recv nc_def_dim from client %d",
		    rank, client_id);
	    cfio_io_def_dim(msg);
	    debug(DEBUG_SERVER, "server %d done nc_def_dim for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_DEF_VAR:
	    debug(DEBUG_SERVER,"server %d recv nc_def_var from client %d",
		    rank, client_id);
	    cfio_io_def_var(msg);
	    debug(DEBUG_SERVER, "server %d done nc_def_var for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_PUT_ATT:
	    debug(DEBUG_SERVER, "server %d recv nc_put_att from client %d",
		    rank, client_id);
	    cfio_io_put_att(msg);
	    debug(DEBUG_SERVER, "server %d done nc_put_att from client %d",
		    rank, client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_ENDDEF:
	    debug(DEBUG_SERVER,"server %d recv nc_enddef from client %d",
		    rank, client_id);
	    cfio_io_enddef(msg);
	    debug(DEBUG_SERVER, "server %d done nc_enddef for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_PUT_VARA:
	    debug(DEBUG_SERVER,"server %d recv nc_put_vara from client %d",
		    rank, client_id);
	    cfio_io_put_vara(msg);
	    debug(DEBUG_SERVER, 
		    "server %d done nc_put_vara_float from client %d\n", 
		    rank, client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_CLOSE:
	    debug(DEBUG_SERVER,"server %d recv nc_close from client %d",
		    rank, client_id);
	    cfio_io_close(msg);
	    debug(DEBUG_SERVER,"server %d received nc_close from client %d\n",
		    rank, client_id);
	    return CFIO_ERROR_NONE;
	default:
	    error("server %d received unexpected msg from client %d",
		    rank, client_id);
	    return CFIO_ERROR_UNEXPECTED_MSG;
    }	
}

static int _init_vars()
{
    int i;
    client_ids = malloc(sizeof(int) * client_num);
    read_addrs = malloc(sizeof(char *) * client_num);
    read_dones = malloc(sizeof(int) * client_num);
    local_lens = malloc(sizeof(int) * client_num);
    copy_lens = malloc(sizeof(int) * client_num);
    comm_dones = malloc(sizeof(int) * client_num);
    msg_cnts = malloc(sizeof(int) * client_num);
    itrs = malloc(sizeof(int) * client_num);

    if (!client_ids || !read_addrs || !read_dones || !local_lens || !copy_lens 
	    || !comm_dones || !msg_cnts || !itrs) {
	error("malloc fail.");
	return CFIO_ERROR_MALLOC;
    }

    cfio_map_get_clients(rank, client_ids);

    for (i = 0; i < client_num; ++i) {
	read_dones[i] = 1;
	local_lens[i] = 0;
	copy_lens[i] = 0;
	comm_dones[i] = 0;
	msg_cnts[i] = 0;
	itrs[i] = 1;
    }
    low_itr = 1;
    low_itr_clt_cnt = client_num;

    msg_clt_cnt = 0;

    return CFIO_ERROR_NONE;
}

static void _free_vars()
{
    free(client_ids);
    free(read_addrs); 
    free(read_dones); 
    free(local_lens); 
    free(copy_lens); 
    free(comm_dones);
    free(msg_cnts);
    free(itrs);
}


void * _io_msgs(void *argv)
{
    int i;
    cfio_msg_t *msg;

    while (comm_done_cnt != client_num || 0 != msg_clt_cnt) {
	pthread_mutex_lock(&io_mutex);
	paused = 1;
	pthread_cond_wait(&io_cond, &io_mutex);
	pthread_mutex_unlock(&io_mutex);

	while (client_num == msg_clt_cnt) {
	    for (i = 0; i < client_num; ++i) {
		msg = cfio_recv_get_first();
		decode(msg); 
		free(msg);
		-- msg_cnts[i];
		if (0 == msg_cnts[i]) {
		    -- msg_clt_cnt;
		}
	    }
	}
    }
    return (void *)0;
}

static void _read_data(int c, cfio_buf_addr_t *addr)
{
#ifdef SVR_ADDR_ONLY
    read_dones[c] = 1;
    local_lens[c] += sizeof(size_t) * 2;
#else
    int length;
    int remote_offset = (int)(read_addrs[c] - addr->start_addr);

    if (addr->used_addr <= addr->free_addr || read_addrs[c] <= addr->free_addr) {
	length = (int)(addr->free_addr - read_addrs[c]);
	length = (length < SERVER_REGION_SIZE - local_lens[c])? 
	    length: SERVER_REGION_SIZE - local_lens[c];


	read_addrs[c] += length;
	if (read_addrs[c] >= addr->free_addr) {
	    read_dones[c] = 1;
	}
    } else {
	// now read_addrs[c] > addr->free_addr
	length = (int)(addr->end_addr - read_addrs[c]);
	length = (length < SERVER_REGION_SIZE - local_lens[c])? 
	    length: SERVER_REGION_SIZE - local_lens[c];

	read_addrs[c] += length;
	if (read_addrs[c] >= addr->end_addr) {
	    read_addrs[c] = addr->start_addr;
	}
    }

    if (length > 0) {
	cfio_rdma_server_read_data(c, remote_offset, length, local_lens[c]);
	local_lens[c] += length;
    }
#endif
}

static void _copy_data_to_list(int c)
{
#ifdef SVR_ADDR_ONLY
    local_lens[c] = 0;
    copy_lens[c] = 0;
#else
    int change = 0, res, i;
    uint32_t func_code;
    char *reg = cfio_rdma_server_get_data(c);
    while (copy_lens[c] + sizeof(size_t) <= local_lens[c]) {
	size_t msg_size = *(size_t*)(&reg[copy_lens[c]]);
	if (copy_lens[c] + msg_size > local_lens[c]) {
	    break;
	} 

	res = cfio_recv_add_msg(client_ids[c], msg_size, &reg[copy_lens[c]], &func_code, itrs[c]); 
	if (CFIO_RECV_BUF_FULL == res) {
	    break;
	} else if (CFIO_ERROR_UNEXPECTED_MSG == res) {
	    printf("itr %d server %d client %d unexpected msg. \n", itrs[c], rank, client_ids[c]);
	    exit(1);
	}

	msg_cnts[c] ++;
	if (1 == msg_cnts[c]) {
	    ++msg_clt_cnt;
	}
	copy_lens[c] += msg_size;
	change = 1;
    }

    if (change) {
	memcpy(&reg[0], &reg[copy_lens[c]], local_lens[c] - copy_lens[c]); // replace with follow
	local_lens[c] = local_lens[c] - copy_lens[c];
	copy_lens[c] = 0;
	if (copy_lens[c] >= local_lens[c]) {
	    local_lens[c] = 0;
	    copy_lens[c] = 0;
	}
    }
#endif
}

static void * cfio_comm(void *argv)
{
    int clt, ret, i;
    cfio_msg_t *msg;
    double IO_time = 0.0, time_temp;
    int handled_reqs;

#ifdef SEND_ADDR
    for (i = 0; i < client_num; ++i) {
	cfio_rdma_server_recv_addr(i);
    }
#endif

    clt = 0;
    while (comm_done_cnt != client_num) {
	handled_reqs = cfio_rdma_server_wait_some();

	// find next client which does not finish its current communication interaction and IO
	while (1) {
	    clt = (clt + 1) % client_num;
	    if (itrs[clt] == low_itr && !comm_dones[clt]) {
		break;
	    }
	}

	cfio_buf_addr_t *addr = (cfio_buf_addr_t *)cfio_rdma_server_get_addr(clt);

	if ( read_dones[clt] && 
		rdma_buf_empty() && // means copy over
		(ret = cfio_rdma_addr_server_test(addr))) 
	{
	    // addr has been changed by client, means new iteration or end of program
	    if (1 == ret) {
		// 1 == ret, means IO end (1 step over)

		read_addrs[clt] = addr->used_addr;
		read_dones[clt] = 0;

	    } else {
		// 11 == ret, means IO done (all step over)
		comm_dones[clt] = 1;
		++comm_done_cnt;

		continue;
	    } 
	}

	if (!read_dones[clt] && 
		!rdma_buf_full()) 
	{
	    // post request to RDMA read client's data

	    _read_data(clt, addr);

	} 

	if (rdma_buf_exist_msg() && // there are msgs to be copied to msg list
		cfio_rdma_server_test(clt)) // read request posted by clt completed successfully
	{
	    // client's data has been read to local, copy it to msg list
	    
	    _copy_data_to_list(clt);

	    if (rdma_buf_empty() && // copy over
		    read_dones[clt])  
	    {
		// copy completed and no more data to read in this itr, to ack client.

		cfio_rdma_addr_server_clear_signal(addr);

#ifdef SEND_ADDR
		cfio_rdma_server_send_ack(clt);
		cfio_rdma_server_recv_addr(clt);
#else 
		cfio_rdma_server_write_addr(clt);
#endif

		++itrs[clt];
		--low_itr_clt_cnt;
		if (!low_itr_clt_cnt) {
		    ++low_itr;
		    low_itr_clt_cnt = client_num;
		}
	    }
	}

	if (msg_clt_cnt == client_num) {
	    // all clients' data of one iteration has been copied to msg list, IO it
	    
	    time_temp = times_cur();
	    while (client_num == msg_clt_cnt) {
		for (i = 0; i < client_num; ++i) {
		    msg = cfio_recv_get_first();
		    decode(msg); 
		    free(msg);
		    -- msg_cnts[i];
		    if (0 == msg_cnts[i]) {
			-- msg_clt_cnt;
		    }
		}
	    }
	    IO_time += times_cur() - time_temp;
	}
    }

    return (void *)0;
}

int cfio_server_start()
{
    // initialize vars
    _init_vars();

    cfio_comm((void*)0);

    // free vars
    _free_vars();

    return CFIO_ERROR_NONE;
}

int cfio_server_init()
{
    init_time = times_cur();

    int i;
    int ret = 0;
    int x_proc_num, y_proc_num;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if((ret = cfio_recv_init()) < 0)
    {
	error("");
	return ret;
    }
    
    if((ret = cfio_id_init(CFIO_ID_INIT_SERVER)) < 0)
    {
	error("");
	return ret;
    }

    if((ret = cfio_io_init(rank)) < 0)
    {
	error("");
	return ret;
    }

    // times_init();

    // init rdma_server
    client_num = cfio_map_get_client_num_of_server(rank);
    recv_bufs = malloc(sizeof(char *) * client_num);
    if (!recv_bufs) {
	printf("line %d malloc error \n", __LINE__);
	return CFIO_ERROR_MALLOC;
    }
    SERVER_REGION_SIZE = RECV_BUF_SIZE / client_num;
    for (i = 0; i < client_num; ++i) {
	recv_bufs[i] = malloc(sizeof(char) * SERVER_REGION_SIZE);
	if (!recv_bufs[i]) {
	    printf("line %d malloc error \n", __LINE__);
	    return CFIO_ERROR_MALLOC;
	}
    }

    buf_addrs = malloc(sizeof(cfio_buf_addr_t *) * client_num);
    if (!buf_addrs) {
	printf("line %d malloc error \n", __LINE__);
	return CFIO_ERROR_MALLOC;
    }
    for (i = 0; i < client_num; ++i) {
	buf_addrs[i] = malloc(sizeof(cfio_buf_addr_t));
	if (!buf_addrs[i]) {
	    printf("line %d malloc error \n", __LINE__);
	    return CFIO_ERROR_MALLOC;
	}
	cfio_rdma_addr_init(buf_addrs[i], NULL);
    }

    cfio_rdma_server_init(client_num, SERVER_REGION_SIZE, recv_bufs, 
	    sizeof(cfio_buf_addr_t), (char **)buf_addrs);

    return CFIO_ERROR_NONE;
}

int cfio_server_final()
{
    cfio_io_final();
    cfio_id_final();
    cfio_recv_final();

    // times_final();

    cfio_rdma_server_final();

    int i;
    for (i = 0; i < client_num; ++i) {
	free(recv_bufs[i]);
	free(buf_addrs[i]);
    }
    free(recv_bufs);
    free(buf_addrs);

    return CFIO_ERROR_NONE;
}
