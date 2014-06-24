CFIO
====

An I/O library for climate models, named CFIO(Climate Fast I/O).

CFIO provides the same interface and feature as PnetCDF, and adopts an I/O forwarding technique to provide automatic overlapping
of I/O with computing. CFIO performs better than PnetCDF in terms of decreasing the overall running time of the program.


What do you need to build CFIO
---------------------------

You need the following softwares been installed before build CFIO, otherwise you may fail to configure.

1. MPI

2. PnetCDF

3. pthread


How to build CFIO
-----------------

Get into CFIO source directory:

```bash
cd {CFIO dir}
```

### configure ###

```bash
./configure --with-dependency={PnetCDF dir} 
```

Or, you may want to install CFIO into another directory:

```bash
./configure --with-dependency={PnetCDF dir} --prefix={Another dir} 
```

If a warning that looks like "mpi compiler not found" appears, you need to specify "CC" and "FC" when configure: 

(mpiicc and mpiifort are recommended if you have Intel MPI installed on your cluster)

```bash
./configure --with-dependency={PnetCDF dir} CC=mpicc(or mpiicc) FC=mpifort(or mpiifort)
```

An example:

```bash
./configure --with-dependency=/home/bus/esml-soft --prefix=/home/bus/esml-cfio CC=mpiicc FC=mpiifort
```

### install ###

```bash
make
make install
```

Test CFIO
---------

CFIO provides simple tests of both functions and performance. The tests store in {CFIO install dir}/test/client/C/.

```bash
cd {CFIO install dir}/test/client/C
```

### Test CFIO functions ###

```bash
mpirun -n 18 ./func_test
```

If this program end without any error and a nc file name "test.nc" is generated, congratulations! your CFIO performs well.

You can open the "test.nc" and verify the output. It contents two dims of "lat" and "lon", one var of "time_v", and a data array values from 0 to 255:

```bash
ncdump test.nc
```

### Test CFIO performance ###

```bash
mpirun -n {TOTAL_PROC} ./perform_test {LAT_PROC} {LON_PROC}
#for example:
mpirun -n 36 ./perform_test 4 8
```

You can change the CFIO_RATIO and other variables in "test_def.h", but you should ensure that "TOTAL_PROC >= LAT_PROC * LON_PROC * (1 + 1/CFIO_RATIO)" and run "make" again.


How to use CFIO
---------------

CFIO provides the same interface and feature as PnetCDF. [Go to netCDF Documentation](http://www.unidata.ucar.edu/software/netcdf/docs/index.html)

The CFIO interface are declared in {CFIO dir}/src/client/C/cfio.h.

You need only to invoke "cfio_init()" in the begining, "cfio_finalize()" in the end, and "cfio_io_end()" in the end of every iteration.

For example:

```c
MPI_Init(...);
...
cfio_init(...);
...
for () {
  //netCDF interface likes functions
  ...
  cfio_io_end(...);
}
...
cfio_finalize(...);

```

More about CFIO
---------------

[Go to wiki](https://github.com/CFIO/CFIO/wiki)


