/****************************************************************************
 *       Filename:  buffer.c
 *
 *    Description:  ring buffer for msg send and recv
 *
 *        Version:  1.0
 *        Created:  04/11/2012 08:46:06 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <assert.h>

#include "debug.h"
#include "buffer.h"
#include "cfio_error.h"

/**
 * @brief: 
 *
 * @param buf_p: 
 * @param data: 
 * @param size: 
 */
static inline void put_buf_data(cfio_buf_t *buf_p, void *data, size_t size)
{
    if(0 == size)
    {
	return;
    }
    memcpy(buf_p->free_addr, data, size);
}
static inline void get_buf_data(cfio_buf_t *buf_p, void *data, size_t size)
{
    char *_data = data;

    if(0 == size)
    {
	return;
    }

    memcpy(_data, buf_p->used_addr, size);
}


cfio_buf_t *cfio_buf_open(size_t size, int *error)
{
    cfio_buf_t *buf_p;
    
    buf_p = malloc(size + sizeof(cfio_buf_t));

    if(NULL == buf_p)
    {
	SET_ERROR(error, CFIO_ERROR_MALLOC);
	error("malloc for buf fail.");
	return NULL;
    }

    buf_p->magic = CFIO_BUF_MAGIC;
    buf_p->size = size;
    buf_p->start_addr = (char *)buf_p + sizeof(cfio_buf_t);
    buf_p->free_addr = buf_p->used_addr = buf_p->start_addr;
    buf_p->magic2 = CFIO_BUF_MAGIC;

    return buf_p;
}

int cfio_buf_close(cfio_buf_t *buf_p)
{
    if(buf_p)
    {
	free(buf_p);
	buf_p = NULL;
    }

    return CFIO_ERROR_NONE;
}

int cfio_buf_clear(cfio_buf_t *buf_p)
{
    assert(NULL != buf_p);

    assert(buf_p->magic == CFIO_BUF_MAGIC && buf_p->magic2 == CFIO_BUF_MAGIC);

    buf_p->free_addr = buf_p->used_addr = buf_p->start_addr;

    return CFIO_ERROR_NONE;
}

size_t cfio_buf_pack_size(
	size_t size)
{
    return size;
}

int cfio_buf_pack_data(
	void *data, size_t size, cfio_buf_t *buf_p)
{
    assert(NULL != data);
    assert(NULL != buf_p);

    assert(buf_p->magic == CFIO_BUF_MAGIC && buf_p->magic2 == CFIO_BUF_MAGIC);

    assert(free_buf_size(buf_p) >= size);

    put_buf_data(buf_p, data, size);
    use_buf(buf_p, size);

    return CFIO_ERROR_NONE;
}
int cfio_buf_unpack_data(
	void *data, size_t size, cfio_buf_t *buf_p)
{
    assert(NULL != data);
    assert(NULL != buf_p);
    volatile size_t used_size;

    assert(buf_p->magic == CFIO_BUF_MAGIC && buf_p->magic2 == CFIO_BUF_MAGIC);

    assert(used_buf_size(buf_p) >= size);

    get_buf_data(buf_p, data, size);
    free_buf(buf_p, size);
    return CFIO_ERROR_NONE;
}

size_t cfio_buf_pack_array_size(
	int len, size_t size)
{
    return len * (size_t)size + (sizeof(int));
}


int cfio_buf_pack_data_array(
	void *data, int len,
	size_t size, cfio_buf_t *buf_p)
{
    
    /**
     *remember consider len = 0
     **/

    size_t data_size = (size_t)len * size;

    assert(NULL != buf_p);

    assert((buf_p->magic == CFIO_BUF_MAGIC && buf_p->magic2 == CFIO_BUF_MAGIC));

    assert((free_buf_size(buf_p) >= (data_size + sizeof(int))));

    put_buf_data(buf_p, &len, sizeof(int));
    use_buf(buf_p, sizeof(int));
    put_buf_data(buf_p, data, data_size);
    use_buf(buf_p, data_size);

    return CFIO_ERROR_NONE;
}

int cfio_buf_unpack_data_array(
	void **data, int *len, 
	size_t size, cfio_buf_t *buf_p)
{
    assert(NULL != data);
    assert(NULL != buf_p);

    size_t data_size;
    volatile size_t used_size;
    int _len;
    
    assert((buf_p->magic == CFIO_BUF_MAGIC && buf_p->magic2 == CFIO_BUF_MAGIC));
    
    assert(used_buf_size(buf_p) >= sizeof(int));

    get_buf_data(buf_p, &_len, sizeof(int));
    free_buf(buf_p, sizeof(int));
    if(NULL != len)
    {
	*len = _len;
    }

    if(0 == _len)
    {
	return CFIO_ERROR_NONE;
    }

    data_size = _len * size;
    (*data) = malloc(data_size);

    assert(used_buf_size(buf_p) >= data_size);

    //debug(DEBUG_BUF, "data_size = %lu", data_size);

    get_buf_data(buf_p, *data, data_size);
    free_buf(buf_p, data_size);

    return CFIO_ERROR_NONE;
}

int cfio_buf_unpack_data_array_ptr(
	void **data, int *len, 
	size_t size, cfio_buf_t *buf_p)
{
    assert(NULL != data);
    assert(NULL != buf_p);

    size_t data_size;
    volatile size_t used_size;
    int _len;
    
    assert(buf_p->magic == CFIO_BUF_MAGIC && buf_p->magic2 == CFIO_BUF_MAGIC);

    assert(used_buf_size(buf_p) >= sizeof(int));

    get_buf_data(buf_p, &_len, sizeof(int));
    free_buf(buf_p, sizeof(int));
    if(NULL != len)
    {
	*len = _len;
    }
    free_buf(buf_p, sizeof(int));

    data_size = (*len) * size;
    assert(used_buf_size(buf_p) >= data_size);

    (*data) = buf_p->used_addr;

    return CFIO_ERROR_NONE;
}
