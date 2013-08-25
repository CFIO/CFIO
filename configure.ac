AC_INIT([CFIO], [1.20], [never.wencan@gmail.com])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
#AC_PROG_CC
#CC=mpiicc
AC_PROG_FC
#FC=mpiifort

#AM_PATH_CHECK

CPPFLAGS="$CPPFLAGS -mcmodel=medium -shared-intel"
#CPPFLAGS="$CPPFLAGS"

AC_ARG_WITH([dependency],
	    [AC_HELP_STRING([--with-dependency=DIR],
			    [Specify path to all dependent library.])],
	    [if test "$withval" != no; then
	     if test "$withval" != yes; then
		 DEP_DIR=$withval
	     fi
	 fi])

if test -n "$DEP_DIR"; then
    CPPFLAGS="$CPPFLAGS -I$DEP_DIR/include"
    LDFLAGS="$LDFLAGS -L$DEP_DIR/lib -Wl,-rpath=$DEP_DIR/lib"
fi

dnl ---------------------------------------------------------------------
dnl szlib (for hdf5)

#AC_ARG_WITH([szlib],
#			[AC_HELP_STRING([--with-szlib=DIR],
#				[Specify path to external szlib library.])],
#			[if test "$withval" != no; then
#				if test "$withval" != yes; then
#					SZLIB_DIR=$withval
#				fi
#			fi])
#
#if test -n "$SZLIB_DIR"; then
#	#CPPFLAGS="$CPPFLAGS -L$SZLIB_DIR/include"
#	LDFLAGS="$LDFLAGS -L$SZLIB_DIR/lib"
#fi
#	
#AC_CHECK_HEADERS([szlib.h],,AC_MSG_ERROR(can not find szlib.h))
#AC_CHECK_LIB(sz, SZ_BufftoBuffCompress,, AC_MSG_ERROR([invalid szlib]))
#
#dnl -----------------------------------------------
#dnl curl
#
#AC_ARG_WITH([curl],
#	    [AC_HELP_STRING([--with-curl=DIR],
#			    [Specify path to external curl library.])],
#			    [if test "$withval" != no; then
#			     if test "$withval" != yes; then
#				 CURL_DIR=$withval
#			     fi
#			 fi])
#
#if test -n "$CURL_DIR"; then
#    #CPPFLAGS="$CPPFLAGS -I$CURL_DIR/include"
#    LDFLAGS="$LDFLAGS -L$CURL_DIR/lib"
#fi
#
#AC_CHECK_LIB([curl], [curl_version],,AC_MSG_ERROR([invalid curl library]))
#
dnl -----------------------------------------------
dnl hdf5

#AC_ARG_WITH([hdf5],
#	    [AC_HELP_STRING([--with-hdf5=DIR],
#			    [Specify path to external hdf5 library.])],
#			    [if test "$withval" != no; then
#			     if test "$withval" != yes; then
#				 HDF5_DIR=$withval
#			     fi
#			 fi])
#
#if test -n "$HDF5_DIR"; then
#    #CPPFLAGS="$CPPFLAGS -I$HDF5_DIR/include"
#    LDFLAGS="$LDFLAGS -L$HDF5_DIR/lib"
#fi
#
#AC_CHECK_LIB([hdf5], [H5Fclose],,AC_MSG_ERROR([invalid hdf5 library]), [-lsz])
#AC_CHECK_LIB([hdf5_hl], [H5DSis_scale],, AC_MSG_ERROR([invalid hdf5 high-level library]))
#
dnl -----------------------------------------------
dnl pnetcdf

AC_ARG_WITH([pnetcdf],
	    [AC_HELP_STRING([--with-pnetcdf=DIR],
			    [Specify path to pnetcdf library.])],
	    [if test "$withval" != no; then
		if test "withval" != yes; then
		    PNETCDF_DIR=$withval
		fi
	    fi])

if test -n "$PNETCDF_DIR"; then
    CPPFLAGS="$CPPFLAGS -I$PNETCDF_DIR/include"
    LDFLAGS="$LDFLAGS -L$PNETCDF_dir/lib"
fi

AC_CHECK_HEADERS([pnetcdf.h],,AC_MSG_ERROR(can not find pnetcdf.h))
AC_CHECK_LIB([pnetcdf], [ncmpi_close],,AC_MSG_ERROR([invalid pnetcdf library]))

dnl -----------------------------------------------
dnl netcdf

#AC_ARG_WITH([netcdf],
#	    [AC_HELP_STRING([--with-netcdf=DIR],
#			    [Specify path to netcdf library.])],
#	    [if test "$withval" != no; then
#		if test "withval" != yes; then
#		    NETCDF_DIR=$withval
#		fi
#	    fi])
#
#if test -n "$NETCDF_DIR"; then
#    CPPFLAGS="$CPPFLAGS -I$NETCDF_DIR/include"
#    LDFLAGS="$LDFLAGS -L$NETCDF_dir/lib"
#fi
#
#AC_CHECK_HEADERS([netcdf.h],,AC_MSG_ERROR(can not find netcdf.h))
#AC_CHECK_LIB([netcdf], [nc_close],,AC_MSG_ERROR([invalid netcdf library]), [-lpnetcdf -lhdf5_hl -lhdf5 -lcurl -lsz])
#
dnl -----------------------------------------------
dnl pnetcdf

AC_CHECK_LIB([pthread], [pthread_create],,AC_MSG_ERROR([invalid pthread library]))

#AC_MSG_ERROR([$LIBS]);

AC_PROG_LIBTOOL
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile src/client/Makefile src/client/C/Makefile \
		 src/client/Fortran/Makefile test/Makefile test/client/Makefile \
		 test/client/C/Makefile])
AC_OUTPUT


