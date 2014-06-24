/****************************************************************************
 * rdma_server.c
 ***************************************************************************/

#include "rdma_server.h"
#include "map.h"
#include "times.h"
#include "addr.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define rdma_debug(x) do { printf("LINE %d: %s %d %s \n", __LINE__, "SERVER", rank, #x); } while (0)

typedef struct {
    enum {
	MSG_MR,
	MSG_DONE,
	MSG_ADDR,
	MSG_ACK
    } type;

    int rank;

    struct ibv_mr data_mr;
    struct ibv_mr addr_mr;

    cfio_buf_addr_t addr;

} rdma_msg_t;

typedef struct {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
} rdma_ctx_t;

typedef struct {
    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    int connected;

    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    struct ibv_mr *data_mr;
    struct ibv_mr *addr_mr;

    int rank;
    struct ibv_mr peer_data_mr;
    struct ibv_mr peer_addr_mr;

    rdma_msg_t *recv_msg;
    rdma_msg_t *send_msg;

    char *data_region;
    char *addr_region;

    enum {
	SS_INIT,
	SS_MR_SENT,
	SS_RDMA_SENDING,
	SS_FINAL,
	SS_DONE_SENT
    } send_state;

    enum {
	RS_INIT,
	RS_MR_RECV,
	RS_RDMA_RECVING,
	RS_FINAL,
	RS_DONE_RECV
    } recv_state;
} rdma_conn_t;

static void server_get_ib_addr(char *host);
static void connect_to_clients();

static void wait_rdma_event();
static int server_on_event(struct rdma_cm_event *event);
static int server_on_connect_request(struct rdma_cm_id *id);
static int server_on_connection(struct rdma_cm_id *id);
static int server_on_disconnect(struct rdma_cm_id *id);

static void build_connection(struct rdma_cm_id *id);
static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void build_params(struct rdma_conn_param *params);
static void on_connect(rdma_conn_t *conn);
static void destroy_connection(rdma_conn_t *conn);

static void register_memory(rdma_conn_t *conn);
static void post_receives(rdma_conn_t *conn);
static void send_message(rdma_conn_t *conn);
static void send_mr(rdma_conn_t *conn);

static void * poll_cq(void *);
static void on_completion(struct ibv_wc *);
static void poll_cq_one_time(void *);

static void die(const char *reason);

static const int TIMEOUT_IN_MS = 500;
static rdma_ctx_t *s_ctx = NULL;
static struct rdma_cm_id *cm_id = NULL; // local cm_id
static struct rdma_event_channel *ec = NULL;
//static volatile int request_stack_size = 0;
static int request_stack_size = 0;
static int previous_rss = 0; // previous request_stack_size 
static int previous_rss_changed = 1; // previous request_stack_size 
//static volatile int *rest_reqs;
static int *rest_reqs;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static int rank;
static int client_amt; // amount of mapped client

static int max_concurrent_reqs;

static int conn_cnt = 0; // count of struct conn
static rdma_conn_t **rdma_conns; // one conn per client, it's disordered
static rdma_conn_t **ordered_conns; // ordered conns by client rank

//static volatile int ready_mr_cnt = 0; 
static int ready_mr_cnt = 0; 
static int established_cnt = 0; // count of established connections
static int disconn_cnt = 0;

// in server,one region per mapped client;in client,only one region.
char **data_regions;
char **addr_regions;

static int DATA_REGION_SIZE; 
static int ADDR_REGION_SIZE; 

/* ************************************************************************
 * server special functions
 * ***********************************************************************/

void cfio_rdma_server_init(
	int region_amt, 
	int _data_region_size, 
	char **_data_regions, 
	int _addr_region_size, 
	char **_addr_regions) 
{
    int i;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // rdma_debug("init ...");

    client_amt = region_amt;
    max_concurrent_reqs = region_amt;

    DATA_REGION_SIZE = _data_region_size; 
    ADDR_REGION_SIZE = _addr_region_size; 
    data_regions = _data_regions;
    addr_regions = _addr_regions;

    rest_reqs = malloc(sizeof(int) * client_amt);
    for (i = 0; i < client_amt; ++i) {
	rest_reqs[i] = 0;
    }

    rdma_conns = malloc(sizeof(rdma_conn_t *) * client_amt);
    ordered_conns = malloc(sizeof(rdma_conn_t *) * client_amt);

    connect_to_clients();

    // listen to client, and build connections with them
    wait_rdma_event();

    // wait for achievement of all clients' sending and receiveing mr
    cfio_rdma_server_wait_all();
    while (client_amt != ready_mr_cnt);

    // rdma_debug("init successfully");
}

void cfio_rdma_server_final()
{
    int i;

    cfio_rdma_server_wait_all();

    for (i = 0; i < client_amt; ++i) {
	rdma_disconnect(rdma_conns[i]->id);
    }

    wait_rdma_event();

    rdma_destroy_event_channel(ec);

    if (rdma_conns) {
	free(rdma_conns);
	rdma_conns = NULL;
    }
    if (ordered_conns) {
	free(ordered_conns);
	ordered_conns = NULL;
    }

    // rdma_debug("final successfully");
}

void cfio_rdma_server_read_data(
	int client_index, 
	int remote_offset, 
	int length, 
	int local_offset)
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    rdma_conn_t *conn = ordered_conns[client_index];
    wr.wr_id = (uintptr_t)(conn);
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)((char *)conn->peer_data_mr.addr + remote_offset); 
    wr.wr.rdma.rkey = conn->peer_data_mr.rkey;

    sge.addr = (uintptr_t)((char *)conn->data_region + local_offset);
    sge.length = length;
    sge.lkey = conn->data_mr->lkey;

    ++ request_stack_size;
    ++ rest_reqs[client_index];
    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

inline char * cfio_rdma_server_get_data(int client_index)
{
    return ordered_conns[client_index]->data_region;
}

inline void cfio_rdma_server_show_data(int client_index)
{
    char *data = ordered_conns[client_index]->data_region;
    printf("LINE %d: SERVER %d data[%d] %s \n", __LINE__, rank, client_index, data);
}

void cfio_rdma_server_write_addr(int client_index)
{
    // rdma_debug("write addr ...");

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    rdma_conn_t *conn = ordered_conns[client_index];
    wr.wr_id = (uintptr_t)(conn);
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)(conn->peer_addr_mr.addr); 
    wr.wr.rdma.rkey = conn->peer_addr_mr.rkey;

    sge.addr = (uintptr_t)(conn->addr_region);
    sge.length = ADDR_REGION_SIZE;
    sge.lkey = conn->addr_mr->lkey;

    ++ request_stack_size;
    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void cfio_rdma_server_send_ack(int client_index)
{
    rdma_conn_t *conn = ordered_conns[client_index];

#ifdef REGISTER_ON_THE_FLY
    ibv_dereg_mr(conn->send_mr);

    TEST_Z(conn->send_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->send_msg, 
		sizeof(rdma_msg_t), 
		IBV_ACCESS_LOCAL_WRITE));
#endif

    conn->send_msg->type = MSG_ACK;
    memcpy(&(conn->send_msg->addr), conn->addr_region, ADDR_REGION_SIZE);

    send_message(conn);
}

void cfio_rdma_server_recv_addr(int client_index)
{
    rdma_conn_t *conn = ordered_conns[client_index];

    post_receives(conn);
}

inline char * cfio_rdma_server_get_addr(int client_index)
{
    return ordered_conns[client_index]->addr_region;
}

void connect_to_clients()
{
    int i;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(cm_id, (struct sockaddr *)&addr));
    TEST_NZ(rdma_listen(cm_id, 10)); /* backlog=10 is arbitrary */

    // get host ipoib addr
    char host[NI_MAXHOST];
    server_get_ib_addr(host);

    // get host port
    uint16_t port = 0;
    port = ntohs(rdma_get_src_port(cm_id));

    // send addr and port to all mapped clients
    int *client_ids = malloc(sizeof(int) * client_amt);
    if (!client_ids) {
	die("malloc fail");
    }
    cfio_map_get_clients(rank, client_ids);
    for (i = 0; i < client_amt; ++i) {
	MPI_Send(host, NI_MAXHOST, MPI_CHAR, client_ids[i], client_ids[i], MPI_COMM_WORLD);
	MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, client_ids[i], client_ids[i] + 1, MPI_COMM_WORLD);
    }
    free(client_ids);
}

void server_get_ib_addr(char *host)
{
    struct ifaddrs *ifaddr, *ifa;

    if (getifaddrs(&ifaddr) == -1) 
	die("getifaddrs");

    /* Walk through linked list, maintaining head pointer so we
       can free list later */

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
	if (ifa->ifa_addr == NULL)
	    continue ;

	if (!strcmp(ifa->ifa_name, "ib0") && (AF_INET == ifa->ifa_addr->sa_family)) {
	    int s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),  
		    host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
	    if (s != 0) {
		printf("LINE %d: SERVER getnameinfo() failed: %s\n", __LINE__, gai_strerror(s));
		die("SERVER get ib addr failed.");
	    }
	    // printf("server ib0 AF_INET address: %s \n", host);
	    break ;
	}
    }

    freeifaddrs(ifaddr);
}

void wait_rdma_event()
{
    struct rdma_cm_event *event = NULL;
    while (!rdma_get_cm_event(ec, &event)) {
	struct rdma_cm_event event_copy;
	memcpy(&event_copy, event, sizeof(*event));
	rdma_ack_cm_event(event);

	if (server_on_event(&event_copy))
	    break;
    }
}

int server_on_event(struct rdma_cm_event *event)
{
    int r = 0;

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
	// rdma_debug("received connect request");
	r = server_on_connect_request(event->id);
    }
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
	// rdma_debug("connection established");
	r = server_on_connection(event->id);
    }
    else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
//	rdma_debug("disconnected");
	r = server_on_disconnect(event->id);
    }
    else
	die("server_on_event: unknown event.");

    return r;
}

int server_on_connect_request(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;

    build_connection(id);

    build_params(&cm_params);

    char *region = ((rdma_conn_t *)id->context)->data_region;
    sprintf(region, "message from passive/server side with pid %d", getpid());

    TEST_NZ(rdma_accept(id, &cm_params));

    return 0;
}

int server_on_connection(struct rdma_cm_id *id)
{
    on_connect(id->context);
    send_mr(id->context);

    // if built connections with all client, return 1, and break while in cfio_rdma_server_init
    ++established_cnt;
    if (client_amt == established_cnt)
	return 1;

    return 0;
}

int server_on_disconnect(struct rdma_cm_id *id)
{
    destroy_connection(id->context);

    ++ disconn_cnt;
    // if disconnect with all client, return 1, and break while in cfio_rdma_server_final
    if (disconn_cnt == client_amt)
	return 1;

    return 0;
}

/* ************************************************************************
 * common use functions
 * ***********************************************************************/

void build_connection(struct rdma_cm_id *id)
{
    rdma_conn_t *conn;
    struct ibv_qp_init_attr qp_attr;

    build_context(id->verbs);
    build_qp_attr(&qp_attr);

    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

    conn = malloc(sizeof(rdma_conn_t));
    id->context = conn; 
    rdma_conns[conn_cnt] = conn;
    ++ conn_cnt;

    conn->id = id;
    conn->qp = id->qp;

    conn->send_state = SS_INIT;
    conn->recv_state = RS_INIT;

    conn->connected = 0;

    register_memory(conn);

    post_receives(conn);
    cfio_rdma_server_wait_some();
}

void build_context(struct ibv_context *verbs)
{
    if (s_ctx) {
	if (s_ctx->ctx != verbs)
	    die("cannot handle events in more than one context.");

	return;
    }

    s_ctx = (rdma_ctx_t *)malloc(sizeof(rdma_ctx_t));

    s_ctx->ctx = verbs;
    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
    
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
    // TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void build_params(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

void on_connect(rdma_conn_t *conn)
{
    conn->connected = 1;
}

void destroy_connection(rdma_conn_t *conn)
{
    rdma_destroy_qp(conn->id);

    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);
    ibv_dereg_mr(conn->data_mr);
    ibv_dereg_mr(conn->addr_mr);

    if (conn->send_msg) {
	free(conn->send_msg);
	conn->send_msg = NULL;
    }
    if (conn->recv_msg) {
	free(conn->recv_msg);
	conn->recv_msg = NULL;
    }

    rdma_destroy_id(conn->id);

    if (conn) {
	free(conn);
	conn = NULL;
    }
}

void register_memory(rdma_conn_t *conn)
{
    conn->send_msg = malloc(sizeof(rdma_msg_t));
    conn->recv_msg = malloc(sizeof(rdma_msg_t));

    conn->data_region = data_regions[conn_cnt - 1];
    conn->addr_region = addr_regions[conn_cnt - 1];

    TEST_Z(conn->send_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->send_msg, 
		sizeof(rdma_msg_t), 
		IBV_ACCESS_LOCAL_WRITE));

    TEST_Z(conn->recv_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->recv_msg, 
		sizeof(rdma_msg_t), 
		IBV_ACCESS_LOCAL_WRITE));

    TEST_Z(conn->data_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->data_region, 
		DATA_REGION_SIZE, 
		IBV_ACCESS_LOCAL_WRITE));

    TEST_Z(conn->addr_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->addr_region, 
		ADDR_REGION_SIZE, 
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

void send_message(rdma_conn_t *conn)
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t)conn->send_msg;
    sge.length = sizeof(rdma_msg_t);
    sge.lkey = conn->send_mr->lkey;

    while (!conn->connected);
 
    ++ request_stack_size;
    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_mr(rdma_conn_t *conn)
{
    conn->send_msg->rank = rank;
    conn->send_msg->type = MSG_MR;
    memcpy(&conn->send_msg->data_mr, conn->data_mr, sizeof(struct ibv_mr));
    memcpy(&conn->send_msg->addr_mr, conn->addr_mr, sizeof(struct ibv_mr));

    send_message(conn);
    cfio_rdma_server_wait_some();
}

void post_receives(rdma_conn_t *conn)
{
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    wr.wr_id = (uintptr_t)conn;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_msg;
    sge.length = sizeof(rdma_msg_t);
    sge.lkey = conn->recv_mr->lkey;

    ++ request_stack_size;
    TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

/* ************************************************************************
 * get and handle rdma cq
 * ***********************************************************************/

inline void cfio_rdma_server_wait_all()
{
    while (request_stack_size) {
	poll_cq_one_time(NULL);
    }
}

int cfio_rdma_server_wait_some()
{
    int before_rss = request_stack_size;

    if (request_stack_size > max_concurrent_reqs) {
	while (request_stack_size > max_concurrent_reqs / 2) {
	    poll_cq_one_time(NULL);
	}
    }

    if (before_rss != request_stack_size) {
	previous_rss = request_stack_size;
	previous_rss_changed = 1;
	return before_rss - request_stack_size;
    }

    if (!previous_rss_changed && previous_rss == request_stack_size) {
	// unchanged twice
	cfio_rdma_server_wait_all();
	previous_rss = 0;
	previous_rss_changed = 1;
    } else {
	previous_rss = request_stack_size;
	previous_rss_changed = 0;
    }

    return 0;
}

void cfio_rdma_server_wait_one()
{
    if (request_stack_size > 0) {
	poll_cq_one_time(NULL);
    }
}

inline int cfio_rdma_server_test(int clt)
{
    return (rest_reqs[clt])? 0: 1;
}

inline int cfio_rdma_server_test_show(int clt)
{
    printf("server %d to TEST data, clt %d, reqs %d. \n", rank, clt, rest_reqs[clt]);
    return 0;
}

void poll_cq_one_time(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));
    int ret;
    while (ret = ibv_poll_cq(cq, 1, &wc)) {
	on_completion(&wc);
    }
}

void * poll_cq(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1) {
	TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
	ibv_ack_cq_events(cq, 1);
	TEST_NZ(ibv_req_notify_cq(cq, 0));

	while (ibv_poll_cq(cq, 1, &wc)) {
	    on_completion(&wc);
	}
    }

    return NULL;
}

void on_completion(struct ibv_wc *wc)
{
    int i;
    rdma_conn_t *conn = (rdma_conn_t *)(uintptr_t)wc->wr_id;

    for (i = 0; i < client_amt; ++i) {
	if (conn == ordered_conns[i] && rest_reqs[i] > 0) {
	    -- rest_reqs[i];
	    break;
	}
    }

    if (wc->status != IBV_WC_SUCCESS) {
	// die("on_completion: status is not IBV_WC_SUCCESS.");
	rdma_debug("on_completion: status is not IBV_WC_SUCCESS.");
    }

    if (wc->opcode & IBV_WC_RECV) {
	// rdma_debug("recv successfully");

	if (RS_RDMA_RECVING != conn->recv_state)
	    conn->recv_state ++;

	if (MSG_MR == conn->recv_msg->type) {
	    conn->rank = conn->recv_msg->rank;
	    memcpy(&conn->peer_data_mr, &conn->recv_msg->data_mr, sizeof(conn->peer_data_mr));
	    memcpy(&conn->peer_addr_mr, &conn->recv_msg->addr_mr, sizeof(conn->peer_addr_mr));
	    
	    int client_index = cfio_map_get_client_index_of_server(conn->rank);
	    ordered_conns[client_index] = conn;
	} else if (MSG_ADDR == conn->recv_msg->type) {
	    memcpy(conn->addr_region, &(conn->recv_msg->addr), ADDR_REGION_SIZE);
	}
    } else {
	// rdma_debug("sent successfully");

	if (SS_RDMA_SENDING != conn->send_state)
	    conn->send_state ++;
    }

    if (RS_MR_RECV == conn->recv_state && SS_MR_SENT == conn->send_state)
	++ ready_mr_cnt; 

    if (RS_DONE_RECV == conn->recv_state && SS_DONE_SENT == conn->send_state) 
	rdma_disconnect(conn->id);

    -- request_stack_size;
}

void die(const char *reason)
{
    printf("ERROR LINE %d: SERVER %d %s \n", __LINE__, rank, reason);
    exit(EXIT_FAILURE);
}
