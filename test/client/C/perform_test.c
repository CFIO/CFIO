/****************************************************************************
 *       Filename:  perform_test.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/26/2012 10:09:11 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <stdio.h>
#include <assert.h>

#include "mpi.h"
#include "cfio.h"
#include "debug.h"
#include "times.h"
#include "test_def.h"

int main(int argc, char** argv)
{
    int rank, size;
    int ncidp;
    int dim1,var1,i, j, l;

    int LAT_PROC, LON_PROC;
    size_t start[2],count[2];
    char fileName[100];
    char var_name[16];
    int var[VALN];
    double compute_time = 0.0, IO_time = 0.0;

    size_t len = 10;
    MPI_Comm comm = MPI_COMM_WORLD;
    volatile double a;

    if(4 != argc)
    {
	printf("Usage : perform_test LAT_PROC LON_PROC output_dir\n");
	return -1;
    }
    
    LAT_PROC = atoi(argv[1]);
    LON_PROC = atoi(argv[2]);
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    times_init();
    double start_time = times_cur();

    //assert(size == LAT_PROC * LON_PROC);
    //set_debug_mask(DEBUG_SERVER | DEBUG_IO | DEBUG_RECV); 
    //set_debug_mask(DEBUG_MAP); 
    //set_debug_mask(DEBUG_SEND); 
    //if(rank == 0)
    //{
    //    set_debug_mask(DEBUG_MSG | DEBUG_CFIO);
    //}
    //set_debug_mask(DEBUG_SERVER | DEBUG_SENDER);
    start[0] = (rank % LAT_PROC) * (LAT / LAT_PROC);
    start[1] = (rank / LAT_PROC) * (LON / LON_PROC);
    count[0] = LAT / LAT_PROC;
    count[1] = LON / LON_PROC;
    //start[0] = 0;
    //start[1] = rank * (LON / size);
    //count[0] = LAT ;
    //count[1] = LON / size;
    double *fp = malloc(count[0] * count[1] *sizeof(double));

//    printf("Proc %d : start(%lu, %lu) ; count(%lu, %lu)\n", 
//	    rank, start[0], start[1], count[0], count[1]);

    for( i = 0; i< count[0] * count[1]; i++)
    {
	fp[i] = i + rank * count[0] * count[1];
    }

    times_start();
    cfio_init( LAT_PROC, LON_PROC, CFIO_RATIO);
    CFIO_START();
    double start_time = times_cur();
    //printf("Loop : %d\n", LOOP);
    for(i = 0; i < LOOP; i ++)
    {
	times_start();
	if( SLEEP_TIME != 0)
	{
	    for(j = 0; j < 1000000; j ++)
	    {
		for(l = 0; l < 11000; l ++)
		{
		    a = 123124.21312/1231.23123;
		}
	    }
	}
	compute_time += times_end();
	//printf("proc %d, loop %d compute time : %f\n", rank, i, times_end());
	times_start();
	sprintf(fileName,"%s/cfio-%d.nc", argv[3], i);
	int dimids[2];
	cfio_create(fileName, NC_64BIT_OFFSET, &ncidp);
	int lat = LAT;
	cfio_def_dim(ncidp, "lat", LAT,&dimids[0]);
	cfio_def_dim(ncidp, "lon", LON,&dimids[1]);
	////cfio_put_att(ncidp, NC_GLOBAL, "global", NC_CHAR, 6, "global");

	for(j = 0; j < VALN; j++)
	{
	    sprintf(var_name, "time_v%d", j);
	    cfio_def_var(ncidp,var_name, CFIO_DOUBLE, 2,dimids, 
		    start, count, &var[j]);
	//    cfio_put_att(ncidp, var[j], "global", NC_CHAR, 
	//	    strlen(var_name), var_name );
	}
	cfio_enddef(ncidp);

	for(j = 0; j < VALN; j++)
	{
	    cfio_put_vara_double(ncidp,var[j], 2,start, count,fp);
	}
	//cfio_put_vara_float(rank,ncidp,var1, 2,start, count,fp); 
	//cfio_put_vara_float(rank,ncidp,var1, 2,start, count,fp); 

	cfio_close(ncidp);
	//printf("send point : %f\n", times_cur() - start_time);
	cfio_io_end();
	//printf("proc %d send point : %f\n", rank, times_cur() - start_time);
	IO_time += times_end();
	//printf("proc %d, loop %d time : %f\n", rank, i, times_end());
    }
    free(fp);

    //printf("proc %d before cfio final time : %f\n", rank, times_cur() - start_time);
    CFIO_END();
    cfio_finalize();
    printf("proc %d compute time : %f\n", rank, compute_time);
    printf("proc %d IO time : %f\n", rank, IO_time);
    printf("proc %d total time : %f\n", rank, times_end());
    
    MPI_Finalize();
    
    times_final();
    return 0;
}
