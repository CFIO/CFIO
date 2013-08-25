common_dir = ../../common
common = $(common_dir)/buffer.c	$(common_dir)/buffer.h 	    $(common_dir)/debug.c 	\
	 $(common_dir)/debug.h 	$(common_dir)/define.h 	    $(common_dir)/id.c  	\
	 $(common_dir)/id.h  	$(common_dir)/cfio_error.h  $(common_dir)/cfio_types.h  \
	 $(common_dir)/map.c  	$(common_dir)/map.h  	    $(common_dir)/msg.c  	\
	 $(common_dir)/msg.h  	$(common_dir)/quickhash.h   $(common_dir)/quicklist.h  	\
	 $(common_dir)/times.c  $(common_dir)/times.h

server_dir = ../../server
server = $(server_dir)/io.c $(server_dir)/io.h  \
	 $(server_dir)/server.c  $(server_dir)/server.h \
	 $(server_dir)/recv.c  $(server_dir)/recv.h

lib_LIBRARIES = libcfio.a
libcfio_a_SOURCES = cfio.h cfio.c send.h send.c\
		    $(common) $(server)
libcfio_a_CFLAGS = -I$(common_dir) -I$(server_dir)

include_HEADERS = cfio.h $(common_dir)/cfio_types.h $(common_dir)/cfio_error.h


