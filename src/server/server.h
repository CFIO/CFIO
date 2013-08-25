/****************************************************************************
 *       Filename:  server.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/19/2012 01:57:25 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#ifndef _SERVER_H
#define _SERVER_H

/**
 * @brief: init
 *
 * @return: error code
 */
int cfio_server_init();
/**
 * @brief: final 
 *
 * @return: error code
 */
int cfio_server_final();
/**
 * @brief: start cfio server
 *
 * @return: error code
 */
int cfio_server_start();

#endif
