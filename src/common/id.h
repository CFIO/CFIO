/****************************************************************************
 *       Filename:  id.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/27/2012 03:15:06 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _ID_H
#define _ID_H

#include "quicklist.h"
#include "cfio_types.h"

#define MAP_HASH_TABLE_SIZE 1024
#define ASSIGN_HASH_TABLE_SIZE 1024

#define CFIO_ID_INIT_CLIENT	    0
#define CFIO_ID_INIT_SERVER	    1

/* return value of cfio_id_get_* */
#define CFIO_ID_HASH_GET_NULL	    1	/* can't find the client id in Map Hash 
					   Table*/
#define DEFINE_MODE 0
#define DATA_MODE   1

/**
 *  * special id assigned to nc, dim and var when the real server nc ,dim or var id 
 *   * hasn't been created
 *    **/
#define CFIO_ID_NC_INVALID -1   
#define CFIO_ID_DIM_INVALID -1
#define CFIO_ID_VAR_INVALID -1
#define CFIO_ID_DIM_LOCAL_NULL -1 /* when the def_var not be called, the local 
				     dim_len will to set to this value, mean that
				     we don't need to call nc_def_dim */


#define cfio_id_val_entry(ptr, member) \
    (cfio_id_val_t *)((char *)ptr - (size_t)&(((cfio_id_val_t *)0)->member)) 
/** @brief: store recv data in var */
typedef struct
{
    char *buf;		    /* pointer to the data */
    size_t *start;	    /* vector of ndims start index of the variable */
    size_t *count;	    /* vector of ndims count index of the variable */
}cfio_id_data_t;

/* quicklist entry for var and dim's name */
typedef struct
{
    char *name;		    /* name of dim or var */
    int id;		    /* id of dim or var */
    qlist_head_t link;
}cfio_id_client_name_t;

/** @brief: store a nc file information in server */
typedef struct 
{
    int nc_id;		    /* id of nc file */
    int nc_status;	    /* the status of nc file : DEFINE_MODE or DATA_MODE */
}cfio_id_nc_t;

/** @brief: store a dimension information in server */
typedef struct
{
    char *name;		    /* name of the dimension */
    int nc_id;		    /* id of nc file which the dimension belong to */
    int dim_id;		    /* id of the dimension */
    
    int dim_len;	    /* length of the dim */
    int global_dim_len;
}cfio_id_dim_t;
/** @brief: store a variable information in server */
typedef struct
{
    char *name;		    /* name of the dimension */
    int nc_id;		    /* id of nc file which the variable is belong to */
    int var_id;		    /* id of var */
    
    int ndims;		    /* number of dimensions for the variable */
    int client_num;	    /* number of clients */
    //size_t *dims_len;	    /* vector of ndims dimension length for the variable */
    int *dim_ids;	    /* vector of ndims dimension ids for the variable */
    size_t *start;	    /* vector of ndims start index of the variable */
    size_t *count;	    /* vector of ndims count index of the variable */
    cfio_id_data_t 
	*recv_data;	    /* pointer to data vector recieved from client */
    cfio_type data_type;          /* type of data, define in cfio_types.h */
    //size_t ele_size;	    /* size of each element in the variable array */
    qlist_head_t 
	*att_head;	    /* variable attribute list */

}cfio_id_var_t;
/** @brief: store a variable's att */
typedef struct
{
    char *name;		    /* name of the attribute */
    cfio_type xtype;	    /* type of the attribute */
    int len;		    /* len of the attribute data */
    char *data;		    /* data of the attribute */
    qlist_head_t link;
}cfio_id_att_t;

/** @brief: id hash entry key */
typedef struct
{
    int client_nc_id;	/* id of nc file in client */
    int client_dim_id;	/* id of nc dim in client */
    int client_var_id;	/* id of nc var in client */
}cfio_id_key_t;

/** @brief: id hash entry key */
typedef struct
{
    int client_nc_id;	/* id of nc file in client */
    int client_dim_id;	/* id of nc dim in client */
    int client_var_id;	/* id of nc var in client */
    
    /* only in client */
    int client_var_a;	/* amount of defined var in client*/
    int client_dim_a;	/* amount of defined dim in client*/
    qlist_head_t *var_head, *dim_head;
		  /* quicklist to store var and dim name that defined before*/
   
    cfio_id_nc_t *nc;   /* nc file infomation in server */
    cfio_id_dim_t *dim; /* dim infomation in server */
    cfio_id_var_t *var; /* nc var infomation in server */
    qlist_head_t hash_link;	
    qlist_head_t link; /* link for interator */
}cfio_id_val_t;

/**
 * @brief: init 
 *
 * @param flag: indicate whether server or client calls the init function
 *
 * @return: error code
 */
int cfio_id_init(int flag);
/**
 * @brief: finalize
 *
 * @return: error code
 */
int cfio_id_final();
/**
 * @brief: assign a nc id in client
 *
 * @param nc_id: the assigned nc id
 *
 * @return: error code
 */
int cfio_id_assign_nc(int *nc_id);
/**
 * @brief: remove a nc_id from the assign_table, it will be called in cfio_close
 *
 * @param nc_id: the nc_id
 *
 * @return: error code
 */
int cfio_id_remove_nc(int nc_id);
/**
 * @brief: assign a nc dim id in client
 *
 * @param nc_id: the nc file id
 * @param dim_id: the assigned dim id
 *
 * @return: error code
 */
int cfio_id_assign_dim(int nc_id, char *dim_name, int *dim_id);
/**
 * @brief: assign a nc var id in client
 *
 * @param nc_id: the nc file id
 * @param var_id: the assigne var id
 *
 * @return: error code
 */
int cfio_id_assign_var(int nc_id, char *var_name, int *var_id);
/**
 * @brief: get a var id in client
 *
 * @param nc_id: the nc id
 * @param var_name : the var name
 * @param var_id : the got var id
 *
 * @return: error code
 */
int cfio_id_inq_var(int nc_id, char *var_name, int *var_id);
/**
 * @brief: add a new map(client_nc_id->server_nc_id) in server
 *
 * @param client_nc_id: the nc file id in client
 * @param server_nc_id: the nc file id in server
 *
 * @return: error code
 */
int cfio_id_map_nc(int client_nc_id, int server_nc_id);
/**
 * @brief: add a new map((client_nc_id, client_dim_id)->(server_nc_id, 
 *	server_dim_id)) in server
 *
 * @param client_nc_id: the nc file id in client
 * @param client_dim_id: the dim id in client
 * @param server_nc_id: the nc file id in server
 * @param server_dim_id: the dim id in server
 * @param name: name of dim
 * @param dim_len: length of dim
 *
 * @return: error code
 */
int cfio_id_map_dim(
	int client_nc_id, int client_dim_id, 
	int server_nc_id, int server_dim_id,
	char *name, int dim_len);
/**
 * @brief: add a new map((client_nc_id, client_var_id)->(server_nc_id,
 *	server_dim_id)) in server
 *
 * @param client_nc_id: the nc file id in client
 * @param client_var_id: the var id in client
 * @param server_nc_id: the nc file id in server
 * @param client_var_id: the var id in server
 * @param ndims: number of dimensions for the variable 
 * @param dim_dis: id of dimensions for the variable
 * @param start: start index of the variable in the whole variable array
 * @param count: count of the variable
 * @param data_type: type of data 
 * @param client_num: number of the server's client num
 *
 * @return: error code
 */
int cfio_id_map_var(
	char *name, 
	int client_nc_id, int client_var_id,
	int server_nc_id, int server_var_id,
	int ndims, int *dim_ids,
	size_t *start, size_t *count,
	cfio_type data_type, int client_num);
int cfio_id_get_val(
	int client_nc_id, int client_var_id, int client_dim_id,
	cfio_id_val_t **val);
/**
 * @brief: get server_nc_id by client_nc_id in server
 *
 * @param client_nc_id: the nc file id in client
 * @param nc: pointer to the nc file information in server
 *
 * @return: error code
 */
int cfio_id_get_nc( 
	int client_nc_id, cfio_id_nc_t **nc);
/**
 * @brief: get (server_nc_id, server_dim_id) by (client_dim_id) in
 *	server
 *
 * @param client_nc_id: the nc file id in client
 * @param client_dim_id: the dim id in client
 * @param dim: pointer to the dim information in server
 *
 * @return: error code
 */
int cfio_id_get_dim(
	int client_nc_id, int client_dim_id, 
	cfio_id_dim_t **dim);
/**
 * @brief: get (server_nc_id, server_var_id) by (client_var_id) in
 *	server
 *
 * @param client_nc_id: the nc file id in client
 * @param client_var_id: the var id in client
 * @param var: pointer to the variable information in the server
 *
 * @return: error code
 */
int cfio_id_get_var(
	int client_nc_id, int client_var_id, 
	cfio_id_var_t **var);

/**
 * @brief: put part of variable data in the recv data vector which is stored in the 
 *	hash table
 *
 * @param client_nc_id: the nc file id in client
 * @param client_var_id: the variable id in client
 * @param client_index: client data index in the recv data vector
 * @param start: start index of the variable in the whole variable array
 * @param count: count of the variable
 * @param data: pointer to the date
 *
 * @return: error code
 */
int cfio_id_put_var(
	int client_nc_id, int client_var_id,
	int client_index,
	size_t *start, size_t *count, 
	char *data);
/**
 * @brief: merge a variable's recv data into its data
 *
 * @param var: the variable
 *
 * @return: error code
 */
int cfio_id_merge_var_data(cfio_id_var_t *var);

void cfio_id_val_free(cfio_id_val_t *val);

#endif
