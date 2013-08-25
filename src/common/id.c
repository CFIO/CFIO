/****************************************************************************
 *       Filename:  id.c
 *
 *    Description:  manage client's id assign, and server's id map, include
 *		    nc_id, var_id, dim_id
 *
 *        Version:  1.0
 *        Created:  04/27/2012 02:50:33 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <assert.h>

#include "id.h"
#include "cfio_types.h"
#include "cfio_error.h"
#include "debug.h"
#include "times.h"
#include "quickhash.h"
#include "map.h"

static int open_nc_a;  /* amount of opened nc file , assigned as new nc id*/
static struct qhash_table *assign_table; /* used for assign id in client*/
static struct qhash_table *map_table;	/* used for map id in server */

static int _compare_client_name(struct qhash_head *link, void *key)
{
    assert(NULL != key);
    assert(NULL != link);

    cfio_id_client_name_t *name = qlist_entry(link, cfio_id_client_name_t, link);

    if(strcmp((char *)key, name->name) == 0)
    {
	return 1;
    }

    return 0;
}
static int _compare(void *key, struct qhash_head *link)
{
    assert(NULL != key);
    assert(NULL != link);

    cfio_id_val_t *val = qlist_entry(link, cfio_id_val_t, hash_link);

    if(0 == memcmp(key, val, sizeof(cfio_id_key_t)))
    {
        return 1;
    }

    return 0;
}

static int _hash(void *key, int table_size)
{
    cfio_id_key_t *_key = key;
    int a, b, c;

    a = _key->client_nc_id; b = _key->client_dim_id; c = _key->client_var_id;
    final(a, b, c);

    int h = (int)(c & (table_size - 1));

    return h;
}

static void _val_free(cfio_id_val_t *val)
{
    if(NULL != val)
    {
	if(NULL != val->var)
	{
	    if(NULL != val->var->start)
	    {
		free(val->var->start);
		val->var->start = NULL;
	    }
	    if(NULL != val->var->count)
	    {
		free(val->var->count);
		val->var->count = NULL;
	    }
	    if(NULL != val->var->recv_data)
	    {
		free(val->var->recv_data);
		val->var->recv_data = NULL;
	    }
	    free(val->var);
	    val->var = NULL;
	}
	free(val);
	val = NULL;
    }
}

int cfio_id_init(int flag)
{
    open_nc_a = 0;
    assign_table = NULL;
    map_table = NULL;

    switch(flag)
    {
	case CFIO_ID_INIT_CLIENT :
	    assign_table = qhash_init(_compare, _hash, ASSIGN_HASH_TABLE_SIZE);
	    if(assign_table == NULL)
	    {
		error("assign_table init fail.");
		return CFIO_ERROR_HASH_TABLE_INIT;
	    }
	    break;
	case CFIO_ID_INIT_SERVER :
	    map_table = qhash_init(_compare,_hash, MAP_HASH_TABLE_SIZE);
	    if(map_table == NULL)
	    {
		error("map_table init fail.");
		return CFIO_ERROR_HASH_TABLE_INIT;
	    }
	    break;
	default :
	    error("This should not happen.");
	    break;
    }

    return CFIO_ERROR_NONE;
}

int cfio_id_final()
{
    if(NULL != assign_table)
    {
	qhash_destroy_and_finalize(assign_table, cfio_id_val_t, 
		hash_link, _val_free);
	assign_table = NULL;
    }

    if(NULL != map_table)
    {
	qhash_destroy_and_finalize(map_table, cfio_id_val_t, hash_link, _val_free);
	map_table = NULL;
    }

    debug(DEBUG_ID, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_id_assign_nc(int *nc_id)
{
    assert(nc_id != NULL);

    int i;
    cfio_id_key_t key;
    cfio_id_val_t *val;

    open_nc_a ++;
    
    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = open_nc_a;

    val = malloc(sizeof(cfio_id_val_t));
    if(val == NULL)
    {
	error("malloc fail.");
	return CFIO_ERROR_MALLOC;
    }
    memset(val, 0, sizeof(cfio_id_val_t));
    val->client_nc_id = open_nc_a;
    val->var_head = malloc(sizeof(qlist_head_t));
    if(val->var_head == NULL)
    {
	error("malloc fail.");
	return CFIO_ERROR_MALLOC;
    }
    val->dim_head = malloc(sizeof(qlist_head_t));
    if(val->dim_head == NULL)
    {
	error("malloc fail.");
	return CFIO_ERROR_MALLOC;
    }
    INIT_QLIST_HEAD(val->var_head);
    INIT_QLIST_HEAD(val->dim_head);
    qhash_add(assign_table, &key, &(val->hash_link));
    *nc_id = open_nc_a;
    debug(DEBUG_ID, "assign nc_id = %d", *nc_id);

    debug(DEBUG_ID, "success return.");
    return CFIO_ERROR_NONE;
}

int cfio_id_remove_nc(int nc_id)
{
    cfio_id_key_t key;
    struct qhash_head *link;
    
    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = nc_id;

    if(NULL == (link = qhash_search(assign_table, &key)))
    {
	error("nc_id(%d) not found in assign_table.", nc_id);
	return CFIO_ERROR_NC_NO_EXIST;
    }else
    {
	qhash_del(link);
	cfio_id_val_free(qlist_entry(link, cfio_id_val_t, hash_link));
	debug(DEBUG_ID, "success return.");
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_assign_dim(int nc_id, char *dim_name, int *dim_id)
{
    assert(dim_id != NULL);
    
    cfio_id_key_t key;
    cfio_id_val_t *val;
    cfio_id_client_name_t *name_entry;
    struct qhash_head *link;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = nc_id;

    if(NULL == (link = qhash_search(assign_table, &key)))
    {
	error("nc_id(%d) not found in assign_table.", nc_id);
	return CFIO_ERROR_NC_NO_EXIST;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	link = qlist_find(val->dim_head, _compare_client_name, dim_name);
	if(link == NULL)
	{
	    val->client_dim_a ++;
	    *dim_id = val->client_dim_a;
	    name_entry = malloc(sizeof(cfio_id_client_name_t));
	    if(name_entry == NULL)
	    {
		error("malloc fail.");
		return CFIO_ERROR_MALLOC;
	    }

	    name_entry->name = strdup(dim_name);
	    name_entry->id = *dim_id;
	    qlist_add(&name_entry->link, val->dim_head);
	}else
	{
	    name_entry = qlist_entry(link, cfio_id_client_name_t, link);
	    *dim_id = name_entry->id;
	}
	debug(DEBUG_ID, "success return.");
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_assign_var(int nc_id, char *var_name, int *var_id)
{
    assert(var_id != NULL);

    cfio_id_key_t key;
    cfio_id_val_t *val;
    cfio_id_client_name_t *name_entry;
    struct qhash_head *link;
    
    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = nc_id;

    if(NULL == (link = qhash_search(assign_table, &key)))
    {
	error("nc_id(%d) not found in assign_table.", nc_id);
	return CFIO_ERROR_NC_NO_EXIST;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	link = qlist_find(val->var_head, _compare_client_name, var_name);
	if(link == NULL)
	{
	    val->client_var_a ++;
	    *var_id = val->client_var_a;
	    name_entry = malloc(sizeof(cfio_id_client_name_t));
	    if(name_entry == NULL)
	    {
		error("malloc fail.");
		return CFIO_ERROR_MALLOC;
	    }

	    name_entry->name = strdup(var_name);
	    name_entry->id = *var_id;
	    qlist_add(&name_entry->link, val->var_head);
	}else
	{
	    name_entry = qlist_entry(link, cfio_id_client_name_t, link);
	    *var_id = name_entry->id;
	}
	debug(DEBUG_ID, "success return.");
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_inq_var(int nc_id, char *var_name, int *var_id)
{
    assert(var_id != NULL);
	
    cfio_id_key_t key;
    cfio_id_val_t *val;
    cfio_id_client_name_t *name_entry;
    struct qhash_head *link;
    
    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = nc_id;

    if(NULL == (link = qhash_search(assign_table, &key)))
    {
	error("nc_id(%d) not found in assign_table.", nc_id);
	return CFIO_ERROR_NC_NO_EXIST;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	link = qlist_find(val->var_head, _compare_client_name, var_name);
	if(link == NULL)
	{
	    return CFIO_ERROR_VAR_NO_EXIST;
	}else
	{
	    name_entry = qlist_entry(link, cfio_id_client_name_t, link);
	    *var_id = name_entry->id;
	}
	debug(DEBUG_ID, "success return.");
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_map_nc(
	int client_nc_id, int server_nc_id)
{
    cfio_id_key_t key;
    cfio_id_val_t *val;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;

    val = malloc(sizeof(cfio_id_val_t));
    memset(val, 0, sizeof(cfio_id_val_t));
    val->client_nc_id = client_nc_id;
    val->nc = malloc(sizeof(cfio_id_nc_t));
    val->nc->nc_id = server_nc_id;
    val->nc->nc_status = DEFINE_MODE;

    qhash_add(map_table, &key, &(val->hash_link));
    INIT_QLIST_HEAD(&(val->link));

    debug(DEBUG_ID, "map ((%d, 0, 0)->(%d, 0, 0)", 
	     client_nc_id, server_nc_id);

    return CFIO_ERROR_NONE;
}

/**
 * name : addr_copy
 **/
int cfio_id_map_dim(
	int client_nc_id, int client_dim_id, 
	int server_nc_id, int server_dim_id,
	char *name, int dim_len)
{
    cfio_id_key_t key;
    cfio_id_val_t *val;
    cfio_id_val_t *nc_val;
    struct qhash_head *link;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;

    link = qhash_search(map_table, &key);
    nc_val = qlist_entry(link, cfio_id_val_t, hash_link);
    
    key.client_dim_id = client_dim_id;

    val = malloc(sizeof(cfio_id_val_t));
    memset(val, 0, sizeof(cfio_id_val_t));
    val->client_nc_id = client_nc_id;
    val->client_dim_id = client_dim_id;
    val->dim = malloc(sizeof(cfio_id_dim_t));
    memset(val->dim, 0, sizeof(cfio_id_dim_t));
    val->dim->nc_id = server_nc_id;
    val->dim->dim_id = server_dim_id;
    val->dim->name = name;
    val->dim->dim_len = CFIO_ID_DIM_LOCAL_NULL;
    val->dim->global_dim_len = dim_len;

    qhash_add(map_table, &key, &(val->hash_link));
    qlist_add_tail(&(val->link), &(nc_val->link));
    
    debug(DEBUG_ID, "map ((%d, %d, 0)->(%d, %d, 0))",
	    client_nc_id, client_dim_id, server_nc_id, server_dim_id);

    return CFIO_ERROR_NONE;
}

/**
 * name, start, count , dim_ids : addr_copy
 **/
int cfio_id_map_var(
	char *name, 
	int client_nc_id, int client_var_id,
	int server_nc_id, int server_var_id,
	int ndims, int *dim_ids,
	size_t *start, size_t *count,
	cfio_type data_type, int client_num)
{
    int i;
    size_t data_size;
    cfio_id_val_t *nc_val;

    cfio_id_key_t key;
    cfio_id_val_t *val;
    struct qhash_head *link;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;
    
    link = qhash_search(map_table, &key);
    nc_val = qlist_entry(link, cfio_id_val_t, hash_link);
    
    key.client_var_id = client_var_id;

    val = malloc(sizeof(cfio_id_val_t));
    memset(val, 0, sizeof(cfio_id_val_t));
    val->client_nc_id = client_nc_id;
    val->client_var_id = client_var_id;

    val->var = malloc(sizeof(cfio_id_var_t));
    val->var->name = name;
    val->var->nc_id = server_nc_id;
    val->var->var_id = server_var_id;
    val->var->ndims = ndims;
    val->var->client_num = client_num;
    val->var->dim_ids = dim_ids;
    val->var->start = start;
    val->var->count = count;

    assert(client_num > 0);
    val->var->recv_data = malloc(sizeof(cfio_id_data_t) * client_num);
    memset(val->var->recv_data, 0, sizeof(cfio_id_data_t) * client_num);
    val->var->data_type = data_type;

    val->var->att_head = malloc(sizeof(qlist_head_t));
    INIT_QLIST_HEAD(val->var->att_head);

    qhash_add(map_table, &key, &(val->hash_link));
    qlist_add_tail(&(val->link), &(nc_val->link));
    
    debug(DEBUG_ID, "map ((%d, 0, %d)->(%d, 0, %d))",  
	    client_nc_id, client_var_id, server_nc_id, server_var_id);
    debug(DEBUG_ID, "client_num = %d",  
	    client_num);

    return CFIO_ERROR_NONE;
}

int cfio_id_get_val(
	int client_nc_id, int client_var_id, int client_dim_id,
	cfio_id_val_t **val)
{
    cfio_id_key_t key;
    struct qhash_head *link;
    
    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;
    key.client_var_id = client_var_id;
    key.client_dim_id = client_dim_id;

    if(NULL == (link = qhash_search(map_table, &key)))
    {
	debug(DEBUG_ID, "get nc (%d, 0, 0) null", client_nc_id);
	return CFIO_ID_HASH_GET_NULL;
    }else
    {
	*val = qlist_entry(link, cfio_id_val_t, hash_link);
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_get_nc(
	int client_nc_id, cfio_id_nc_t **nc)
{
    cfio_id_key_t key;
    struct qhash_head *link;
    cfio_id_val_t *val;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;

    if(NULL == (link = qhash_search(map_table, &key)))
    {
	debug(DEBUG_ID, "get nc (%d, 0, 0) null", client_nc_id);
	return CFIO_ID_HASH_GET_NULL;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	assert(val->nc != NULL);
	*nc = val->nc;
    
	debug(DEBUG_ID, "get (%d, 0, 0)", client_nc_id);
	
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_get_dim(
	int client_nc_id, int client_dim_id, 
	cfio_id_dim_t **dim)
{
    cfio_id_key_t key;
    struct qhash_head *link;
    cfio_id_val_t *val;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;
    key.client_dim_id = client_dim_id;

    if(NULL == (link = qhash_search(map_table, &key)))
    {
	debug(DEBUG_ID, "get dim (%d, %d, 0) null" ,
		client_nc_id, client_dim_id);
	return CFIO_ID_HASH_GET_NULL;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	*dim = val->dim;
	assert(val->dim != NULL);
	debug(DEBUG_ID, "get (%d, %d, 0)", client_nc_id, client_dim_id);
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_get_var(
	int client_nc_id, int client_var_id, 
	cfio_id_var_t **var)
{
    cfio_id_key_t key;
    struct qhash_head *link;
    cfio_id_val_t *val;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;
    key.client_var_id = client_var_id;

    if(NULL == (link = qhash_search(map_table, &key)))
    {
	debug(DEBUG_ID, "get var (%d, 0, %d) null",
		client_nc_id, client_var_id);
	return CFIO_ID_HASH_GET_NULL;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	assert(val->var != NULL);
	*var = val->var;
	debug(DEBUG_ID, "get (%d, 0, %d)", client_nc_id, client_var_id);
	return CFIO_ERROR_NONE;
    }
}
/**
 * start, count , data : addr_copy
 **/

int cfio_id_put_var(
	int client_nc_id, int client_var_id,
	int client_index,
	size_t *start, size_t *count, 
	char *data)
{
    assert(NULL != start);
    assert(NULL != count);
    assert(NULL != data);

    cfio_id_key_t key;
    cfio_id_val_t *val;
    struct qhash_head *link;
    cfio_id_var_t *var;
    int src_indx, dst_index;
    int i;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;
    key.client_var_id = client_var_id;

    if(NULL == (link = qhash_search(map_table, &key)))
    {
	debug(DEBUG_ID, "Can't find var (%d, 0, %d)", 
		client_nc_id, client_var_id);
	return CFIO_ID_HASH_GET_NULL;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	var = val->var;

	//for(i = 0; i < var->ndims; i ++)
	//{
	//    if(start[i] + count[i] > var->start[i] + var->count[i] ||
	//	    start[i] < var->start[i])
	//    {
	//	debug(DEBUG_ID, "Index exceeds : (start[%d] = %lu), "
	//		"(count[%d] = %lu) ; (var(%d, 0, %d) : (start[%d] "
	//		"= %lu), (count[%d] = %lu", i, start[i], i, count[i],
	//		client_nc_id, client_var_id, 
	//		i, var->start[i], i, var->count[i]);
	//	return CFIO_ERROR_EXCEED_BOUND;
	//    }
	//}

	//float *_data = data;
	//for(i = 0; i < 4; i ++)
	//{
	//    printf("%f, ", _data[i]);
	//}
	//printf("\n");

	if(NULL != var->recv_data[client_index].buf)
	{
	    free(var->recv_data[client_index].buf);
	}
	if(NULL != var->recv_data[client_index].start)
	{
	    free(var->recv_data[client_index].start);
	}
	if(NULL != var->recv_data[client_index].count)
	{
	    free(var->recv_data[client_index].count);
	}
	var->recv_data[client_index].buf = data;
	var->recv_data[client_index].start = start;
	var->recv_data[client_index].count = count;
	debug(DEBUG_ID, "client_index = %d", client_index);

	debug(DEBUG_ID, "put var ((%d, 0, %d)", client_nc_id, client_var_id);
	return CFIO_ERROR_NONE;
    }
}

int cfio_id_put_att(
	int client_nc_id, int client_var_id,
	char *name, cfio_type xtype, int len, char *data)
{
    cfio_id_key_t key;
    cfio_id_val_t *val;
    struct qhash_head *link;
    cfio_id_var_t *var;
    cfio_id_att_t *att;
    int i;

    memset(&key, 0, sizeof(cfio_id_key_t));
    key.client_nc_id = client_nc_id;
    key.client_var_id = client_var_id;

    if(NULL == (link = qhash_search(map_table, &key)))
    {
	debug(DEBUG_ID, "Can't find var (%d, 0, %d)", 
		client_nc_id, client_var_id);
	return CFIO_ID_HASH_GET_NULL;
    }else
    {
	val = qlist_entry(link, cfio_id_val_t, hash_link);
	var = val->var;
	
	att = malloc(sizeof(cfio_id_att_t));
	att->name = name;
	att->xtype = xtype;
	att->len = len;
	att->data = data;
	qlist_add_tail(&(att->link), var->att_head);

	debug(DEBUG_ID, "put att(%s)", att->name);
	
	return CFIO_ERROR_NONE;
    }
} 

void cfio_id_val_free(cfio_id_val_t *val)
{
    int i;
    cfio_id_data_t *recv_data;
    cfio_id_att_t *att, *next;
    cfio_id_client_name_t *name, *name_next;

    debug(DEBUG_ID, "start free.");

    if(NULL != val)
    {
	if(NULL != val->nc)
	{
	    free(val->nc);
	    val->nc = NULL;
	}
	if(NULL != val->dim)
	{
	    if(NULL != val->dim->name)
	    {
		free(val->dim->name);
		val->dim->name = NULL;
	    }
	    free(val->dim);
	    val->dim = NULL;
	}
	if(NULL != val->var)
	{
	    if(NULL != val->var->name)
	    {
		free(val->var->name);
		val->var->name = NULL;
	    }
	    if(NULL != val->var->dim_ids)
	    {
		free(val->var->dim_ids);
		val->var->dim_ids = NULL;
	    }
	    if(NULL != val->var->start)
	    {
		free(val->var->start);
		val->var->start = NULL;
	    }
	    if(NULL != val->var->count)
	    {
		free(val->var->count);
		val->var->count = NULL;
	    }
	    recv_data = val->var->recv_data;
	    if(NULL != recv_data)
	    {
		for(i = 0; i < val->var->client_num; i ++)
		{
		    if(NULL != recv_data[i].buf)
		    {
			free(recv_data[i].buf);
			recv_data[i].buf = NULL;
		    }
		    if(NULL != recv_data[i].start)
		    {
			free(recv_data[i].start);
			recv_data[i].start = NULL;
		    }
		    if(NULL != recv_data[i].count)
		    {
			free(recv_data[i].count);
			recv_data[i].count = NULL;
		    }
		}
		free(recv_data);
		recv_data = NULL;
	    }
	    if(NULL != val->var->att_head)
	    {
		qlist_for_each_entry_safe(att, next, val->var->att_head, link)
		{
		    assert(att != NULL);
		   if(att->name != NULL)
		   {
		       free(att->name);
		       att->name = NULL;
		   }
		   if(att->data != NULL)
		   {
		       free(att->data);
		       att->data = NULL;
		   }
		   free(att);
		}
		free(val->var->att_head);
		val->var->att_head = NULL;
	    }
	    free(val->var);
	    val->var = NULL;
	} /* end var */
	/* free var name list and dim name list */
	if(val->var_head != NULL)
	{
	    qlist_for_each_entry_safe(name, name_next, val->var_head, link)
	    {
		if(name->name != NULL)
		{
		    free(name->name);
		    name->name = NULL;
		}
		free(name);
	    }
	    free(val->var_head);
	    val->var_head = NULL;
	}
	if(val->dim_head != NULL)
	{
	    qlist_for_each_entry_safe(name, name_next, val->dim_head, link)
	    {
		if(name->name != NULL)
		{
		    free(name->name);
		    name->name = NULL;
		}
		free(name);
	    }
	    free(val->dim_head);
	    val->dim_head = NULL;
	}
	
	free(val);
	val = NULL;
    }
    debug(DEBUG_ID, "success return.");
}
	
