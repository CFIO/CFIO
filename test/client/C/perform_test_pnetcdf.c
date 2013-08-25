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
#include "debug.h"
#include "pnetcdf.h"
#include "times.h"
#include "test_def.h"

int main(int argc, char** argv)
{
    int rank, size;
    char *path = "./output/test";
    int ncidp;
    int dim1,var1,i, j, l;
    MPI_Info info;
    int ret;
    int extra;
    MPI_Group group, server_group;
    MPI_Comm server_comm;

    int LAT_PROC, LON_PROC;
    MPI_Offset start[2],count[2];
    int var[VALN];
    char var_name[16];
    double compute_time = 0.0, IO_time = 0.0;
    int *ranks;

    size_t len = 10;
    volatile double a;
    MPI_Comm comm = MPI_COMM_WORLD;

    if(5 != argc)
    {
	printf("Usage : perform_test_pnetcdf LAT_PROC LON_PROC output_dir\n");
	return -1;
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);
    
    times_init();
    
    LAT_PROC = atoi(argv[1]);
    LON_PROC = atoi(argv[2]);
    extra = atoi(argv[4]);
    
    assert(size == LAT_PROC * LON_PROC + extra);
   
    if(extra > 0)
    {
	size = LAT_PROC * LON_PROC;

	ranks = malloc(size * sizeof(int));
	for(i = 0; i < size; i ++)
	{
	    ranks[i] = i + extra;
	}
	MPI_Comm_group(MPI_COMM_WORLD, &group);
	MPI_Group_incl(group, size, ranks, &server_group);
	MPI_Comm_create(MPI_COMM_WORLD, server_group, &comm);
	free(ranks);
    }else
    {
	comm = MPI_COMM_WORLD;
    }

    if(rank >= extra)
    {
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);
    //printf("rank : %d, size : %d\n", rank, size);
    //set_debug_mask(DEBUG_USER | DEBUG_MSG | DEBUG_CFIO | DEBUG_ID); 
    //set_debug_mask(DEBUG_ID); 
    //set_debug_mask(DEBUG_USER); 
    start[0] = (rank % LAT_PROC) * (LAT / LAT_PROC);
    start[1] = (rank / LAT_PROC) * (LON / LON_PROC);
    count[0] = LAT / LAT_PROC;
    count[1] = LON / LON_PROC;
    /*line*/
    //start[0] = 239;
    //start[1] = rank * 144;
    //count[0] = 1444 ;
    //count[1] = 146;
    /*row*/
    //start[0] = rank * (LAT / size);
    //start[1] = 0;
    //count[0] = LAT /size;
    //count[1] = LON;
    double *fp = malloc(count[0] * count[1] *sizeof(double));

    for( i = 0; i< count[0] * count[1]; i++)
    {
        fp[i] = i + rank * count[0] * count[1];
    }
    
    times_start();

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
        times_start();
        char fileName[100];
        sprintf(fileName,"%s/pnetcdf-%d.nc", argv[3], i);
        int dimids[2];
        debug_mark(DEBUG_USER);
        MPI_Info_create(&info);
        //MPI_Info_set(info, "romio_ds_write", "disable");
        //MPI_Info_set(info, "ind_wr_buffer_size", "16777216");
        debug_mark(DEBUG_USER);
        //ncmpi_create(MPI_COMM_WORLD, fileName, NC_64BIT_OFFSET, info, &ncidp);
        ncmpi_create(comm, fileName, NC_64BIT_OFFSET, MPI_INFO_NULL, &ncidp);
        debug_mark(DEBUG_USER);
        int lat = LAT;
        ncmpi_def_dim(ncidp, "lat", lat,&dimids[0]);
        ncmpi_def_dim(ncidp, "lon", LON,&dimids[1]);

        for(j = 0; j < VALN; j++)
        {
            sprintf(var_name, "time_v%d", j);
            ncmpi_def_var(ncidp,var_name, NC_DOUBLE, 2,dimids, &var[j]);
        }
        debug_mark(DEBUG_USER);
        ncmpi_enddef(ncidp);

        debug_mark(DEBUG_USER);
        for(j = 0; j < VALN; j++)
        {
            ret = ncmpi_put_vara_double_all(ncidp,var[j], start, count,fp);
        	//printf("%s\n", ncmpi_strerror(ret));
        }
        debug_mark(DEBUG_USER);

        ncmpi_close(ncidp);
        IO_time += times_end();
        //printf("proc %d, loop %d time : %f\n", rank, i, times_end());
    }

    free(fp);
    printf("proc %d total time : %f\n", rank, times_end());
    printf("proc %d compute time : %f\n", rank, compute_time);
    printf("proc %d IO time : %f\n", rank, IO_time);
    }
    MPI_Finalize();
    times_final();
    return 0;
}

