LDADD = ../../../src/client/C/libcfio.a 
AM_LDFLAGS = -mt_mpi
AM_CFLAGS = -I../../../src/client/C -I../../../src/common

bin_PROGRAMS = func_test perform_test_pnetcdf perform_test 
func_test_SOURCES = func_test.c test_def.h
perform_test_SOURCES = perform_test.c

perform_test_pnetcdf_SOURCES = perform_test_pnetcdf.c test_def.h
