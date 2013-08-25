/****************************************************************************
 *       Filename:  debug.h
 *
 *    Description:  define for debug mask
 *
 *        Version:  1.0
 *        Created:  12/28/2011 03:24:20 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _DEBUG_H
#define _DEBUG_H

#include <stdio.h>
#include <stdint.h>

#define ENABLE_DEBUG
//#define SVR_RECV_ONLY	/* server only do recieve */
//#define SVR_UNPACK_ONLY	/* server recieve and unpack data from buffer, 
//			   but handle client done only */

#define DEBUG_NONE	((uint32_t)0)
#define DEBUG_USER  	((uint32_t)1 << 0)
#define DEBUG_CFIO  	((uint32_t)1 << 1)
#define DEBUG_PACK  	((uint32_t)1 << 2)
#define DEBUG_TIME  	((uint32_t)1 << 3)
#define DEBUG_MSG   	((uint32_t)1 << 4)
#define DEBUG_IO    	((uint32_t)1 << 5)
#define DEBUG_MAP   	((uint32_t)1 << 6)
#define DEBUG_ID    	((uint32_t)1 << 7)
#define DEBUG_BUF   	((uint32_t)1 << 8)
#define DEBUG_SERVER	((uint32_t)1 << 9)
#define DEBUG_SEND	((uint32_t)1 << 10)
#define DEBUG_RECV	((uint32_t)1 << 11)

extern int debug_mask;

#ifdef ENABLE_DEBUG
/* try to avoid function call overhead by checking masks in macro */
#define debug(mask, format, f...)                  \
    if (debug_mask & mask)                         \
    {                                                     \
        printf("[%s, %s, %d]: " format "\n", __FILE__ , __func__, __LINE__ , ##f); \
    }                                                     
#else
#define debug(mask, format, f...) \
    do {} while(0)
#endif

#define set_debug_mask(mask) debug_mask = mask
#define add_debug_mask(mask) debug_mask |= mask

#define debug_mark(mask) debug(mask, "MARK")

#define error(format, f...) \
    printf("[%s, %s, %d]: "format "\n", __FILE__, __func__, __LINE__, ##f);

#endif
