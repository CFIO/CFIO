/****************************************************************************
 * rdma_client.c
 ***************************************************************************/

#include "rdma_client.h"
#include "times.h"
#include "addr.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define rdma_debug(x) do { printf("LINE %d: CLIENT %d %s \n", __LINE__, rank, #x); } while (0)

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

void wait_rdma_event();
void connect_to_server();

static int client_on_event(struct rdma_cm_event *event);
static int client_on_addr_resolved(struct rdma_cm_id *id);
static int client_on_route_resolved(struct rdma_cm_id *id);
static int client_on_connection(struct rdma_cm_id *id);
static int client_on_disconnect(struct rdma_cm_id *id);

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

static void die(const char *reason);

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t resume_cond = PTHREAD_COND_INITIALIZER;
static int paused = 0;

static const int TIMEOUT_IN_MS = 500;
static rdma_ctx_t *s_ctx = NULL;
static struct rdma_cm_id *cm_id = NULL; // local cm_id
static struct rdma_event_channel *ec = NULL;

static int request_stack_size = 0; // number of unfinished rdma requests
static int ready_mr_cnt = 0; // 

static rdma_conn_t *rdma_conn; // one conn per client 

char *data_region;
char *addr_region;

static int DATA_REGION_SIZE; 
static int ADDR_REGION_SIZE; 

static int rank;

/* ************************************************************************
 * client special functions
 * ***********************************************************************/

void cfio_rdma_client_init(
	int data_region_size, 
	char *_data_region, 
	int addr_region_size, 
	char *_addr_region) 
{
    double begin, end;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    DATA_REGION_SIZE = data_region_size; 
    ADDR_REGION_SIZE = addr_region_size;
    data_region = _data_region;
    addr_region = _addr_region;

    // connect to server
    connect_to_server();

    wait_rdma_event();

    // wait for achievement of sending and receiving peer mr
    cfio_rdma_client_wait(NULL);
    while (!ready_mr_cnt);

    // rdma_debug("init successfully");
}

void cfio_rdma_client_final()
{
    cfio_rdma_client_wait(NULL);

    double begin, end;

    rdma_disconnect(rdma_conn->id);

    wait_rdma_event();

    rdma_destroy_event_channel(ec);
    // rdma_debug("final successfully");
}

void cfio_rdma_client_write_data(
	int remote_offset, 
	int length, 
	int local_offset)
{
    if (remote_offset < 0 || remote_offset + length > DATA_REGION_SIZE) 
	die("RDMA out of region");

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    rdma_conn_t *conn = rdma_conn;
    wr.wr_id = (uintptr_t)(conn);
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)((char *)conn->peer_data_mr.addr + remote_offset); 
    wr.wr.rdma.rkey = conn->peer_data_mr.rkey;

    sge.addr = (uintptr_t)(conn->data_region + local_offset);
    sge.length = length;
    sge.lkey = conn->data_mr->lkey;

    ++ request_stack_size;
    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void cfio_rdma_client_write_addr()
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    rdma_conn_t *conn = rdma_conn;
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

void cfio_rdma_client_send_addr()
{
    rdma_conn_t *conn = rdma_conn;

#ifdef REGISTER_ON_THE_FLY
    ibv_dereg_mr(conn->send_mr);

    TEST_Z(conn->send_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->send_msg, 
		sizeof(rdma_msg_t), 
		IBV_ACCESS_LOCAL_WRITE));
#endif

    conn->send_msg->type = MSG_ADDR;
    memcpy((char *)&(conn->send_msg->addr), conn->addr_region, ADDR_REGION_SIZE);

    send_message(conn);
}

void cfio_rdma_client_recv_ack() 
{
    rdma_conn_t *conn = rdma_conn;

    post_receives(conn);
}

inline void cfio_rdma_client_show_data()
{
    printf("LINE %d: CLIENT %d data %s \n", __LINE__, rank, data_region);
}

void connect_to_server()
{
    // recv addr and port from server
    int server_rank = cfio_map_get_server_of_client(rank);
    char server_ip[NI_MAXHOST];
    unsigned short server_port_int;
    char server_port[11];
    MPI_Status sta;
    MPI_Recv(server_ip, NI_MAXHOST, MPI_CHAR, server_rank, rank, MPI_COMM_WORLD, &sta);
    MPI_Recv(&server_port_int, 1, MPI_UNSIGNED_SHORT, server_rank, rank + 1, MPI_COMM_WORLD, &sta);
    sprintf(server_port, "%hu", server_port_int);

    struct addrinfo *addr;
    TEST_NZ(getaddrinfo(server_ip, server_port, NULL, &addr));
    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));
    freeaddrinfo(addr);
}

void wait_rdma_event()
{
    struct rdma_cm_event *event = NULL;
    while (!rdma_get_cm_event(ec, &event)) {
	struct rdma_cm_event event_copy;
	memcpy(&event_copy, event, sizeof(*event));
	rdma_ack_cm_event(event);

	if (client_on_event(&event_copy))
	    break;
    }
}

int client_on_event(struct rdma_cm_event *event)
{
    int r = 0;

    if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
	r = client_on_addr_resolved(event->id);
    } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
	r = client_on_route_resolved(event->id);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
	r = client_on_connection(event->id);
    } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
	r = client_on_disconnect(event->id);
    } else
	die("client_on_event: unknown event.");

    return r;
}

int client_on_addr_resolved(struct rdma_cm_id *id)
{
    build_connection(id);
    TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

    return 0;
}

int client_on_route_resolved(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;
    build_params(&cm_params);
    TEST_NZ(rdma_connect(id, &cm_params));

    return 0;
}

int client_on_connection(struct rdma_cm_id *id)
{
    on_connect(id->context);
    send_mr(id->context);

    // if built connection, return 1, and break the while in wait_rdma_event()
    return 1;
}

int client_on_disconnect(struct rdma_cm_id *id)
{
    destroy_connection(id->context);
    return 1; 
}

/* ************************************************************************
 * common use functions, about connection
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
    rdma_conn = conn;

    conn->id = id;
    conn->qp = id->qp;

    conn->send_state = SS_INIT;
    conn->recv_state = RS_INIT;

    conn->connected = 0;

    register_memory(conn);

    post_receives(conn);
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
//    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
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

    conn->data_region = data_region;
    conn->addr_region = addr_region;

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

    TEST_Z(conn->addr_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->addr_region, 
		ADDR_REGION_SIZE, 
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

    TEST_Z(conn->data_mr = ibv_reg_mr(
		s_ctx->pd, 
		conn->data_region, 
		DATA_REGION_SIZE, 
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ));
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

inline void cfio_rdma_client_wait(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (request_stack_size) {
	TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
	ibv_ack_cq_events(cq, 1);
	TEST_NZ(ibv_req_notify_cq(cq, 0));

	while (ibv_poll_cq(cq, 1, &wc)) {
	    on_completion(&wc);
	}
    }
}

inline void cfio_rdma_client_poll_pause()
{
    if (!paused) {
	pthread_mutex_lock(&mutex);
	paused = 1;
	pthread_mutex_unlock(&mutex);
    }
}

inline void cfio_rdma_client_poll_resume()
{
    if (paused) {
	pthread_mutex_lock(&mutex);
	paused = 0;
	pthread_cond_signal(&resume_cond);
	pthread_mutex_unlock(&mutex);
    }
}

void * poll_cq(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1) {
	if (!paused) {
	    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
	    ibv_ack_cq_events(cq, 1);
	    TEST_NZ(ibv_req_notify_cq(cq, 0));

	    while (ibv_poll_cq(cq, 1, &wc)) {
		on_completion(&wc);
	    }
	} else {
	    pthread_mutex_lock(&mutex);
	    pthread_cond_wait(&resume_cond, &mutex);
	    pthread_mutex_unlock(&mutex);
	}
    }

    return NULL;
}

void on_completion(struct ibv_wc *wc)
{
    rdma_conn_t *conn = (rdma_conn_t *)(uintptr_t)wc->wr_id;

    if (wc->status != IBV_WC_SUCCESS) {
	rdma_debug("on_completion: status is not IBV_WC_SUCCESS.");
    }

    if (wc->opcode & IBV_WC_RECV) {
	if (RS_RDMA_RECVING != conn->recv_state)
	    conn->recv_state ++;

	if (MSG_MR == conn->recv_msg->type) {
	    conn->rank = conn->recv_msg->rank;
	    memcpy(&conn->peer_data_mr, &conn->recv_msg->data_mr, sizeof(conn->peer_data_mr));
	    memcpy(&conn->peer_addr_mr, &conn->recv_msg->addr_mr, sizeof(conn->peer_addr_mr));
	} else if (MSG_ACK == conn->recv_msg->type) {
	    memcpy(conn->addr_region, &conn->recv_msg->addr, ADDR_REGION_SIZE);
	}
    } else {
	if (SS_RDMA_SENDING != conn->send_state)
	    conn->send_state ++;
    }

    if (RS_MR_RECV == conn->recv_state && SS_MR_SENT == conn->send_state)
	++ ready_mr_cnt; 

    if (RS_DONE_RECV == conn->recv_state && SS_DONE_SENT == conn->send_state) { 
	rdma_disconnect(conn->id);
    }

    -- request_stack_size;
}

void die(const char *reason)
{
    printf("ERROR LINE %d: CLIENT %d %s \n", __LINE__, rank, reason);
    exit(EXIT_FAILURE);
}
