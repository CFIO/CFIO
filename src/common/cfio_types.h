/****************************************************************************
 *       Filename:  cfio_types.h
 *
 *    Description:  define data types in cfio
 *
 *        Version:  1.0
 *        Created:  08/20/2012 03:30:19 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _CFIO_TYPES_H
#define _CFIO_TYPES_H

#include <pnetcdf.h>

typedef enum
{
    CFIO_BYTE   =	1,
    CFIO_CHAR   =	2,
    CFIO_SHORT  =	3,
    CFIO_INT    =	4,
    CFIO_FLOAT  =	5,
    CFIO_DOUBLE =	6,
}cfio_type;

static inline nc_type cfio_type_to_nc(cfio_type type)
{
    //return NC_BYTE;
    switch(type)
    {
        case CFIO_BYTE :
            return NC_BYTE;
        case CFIO_CHAR :
            return NC_CHAR;
        case CFIO_SHORT :
            return NC_SHORT;
        case CFIO_INT :
            return NC_INT;
        case CFIO_FLOAT :
            return NC_FLOAT;
        case CFIO_DOUBLE :
            return NC_DOUBLE;
	default :
	    return NC_NAT;
    }
}

#define cfio_types_size(size, type) \
    do{				    \
    switch(type) {		    \
	case CFIO_BYTE :	    \
	    size = 1;		    \
	    break;		    \
	case CFIO_CHAR :	    \
	    size = 1;		    \
	    break;		    \
	case CFIO_SHORT :	    \
	    size = sizeof(short);   \
	    break;		    \
	case CFIO_INT :		    \
	    size = sizeof(int);	    \
	    break;		    \
	case CFIO_FLOAT :	    \
	    size = sizeof(float);   \
	    break;		    \
	case CFIO_DOUBLE :	    \
	    size = sizeof(double);  \
	    break;		    \
    }} while(0)

#endif
