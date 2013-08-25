module cfio
implicit none

integer, parameter :: CFIO_PROC_CLIENT = 1
integer, parameter :: CFIO_PROC_SERVER = 2
integer, parameter :: CFIO_PROC_BLANK = 3

integer :: char_len = 128
integer :: array_len = 128

integer, parameter :: cfio_byte   = 1
integer, parameter :: cfio_char   = 2
integer, parameter :: cfio_short  = 3
integer, parameter :: cfio_int	  = 4
integer, parameter :: cfio_float  = 5
integer, parameter :: cfio_double = 6

interface cfio_put_att
    module procedure cfio_put_att_str
    module procedure cfio_put_att_int
    module procedure cfio_put_att_real
    module procedure cfio_put_att_double
end interface

interface cfio_put_vara
    module procedure cfio_put_vara_real
    module procedure cfio_put_vara_double
    module procedure cfio_put_vara_int
end interface

contains

integer(4) function cfio_init(x_proc_num, y_proc_num, ratio)
    implicit none
    integer(4), intent(in) :: x_proc_num, y_proc_num, ratio

    call cfio_init_c(x_proc_num, y_proc_num, ratio, cfio_init)

end function

integer(4) function cfio_finalize()
    implicit none

    call cfio_finalize_c(cfio_finalize)

end function

integer(4) function cfio_proc_type()
    implicit none

    call cfio_proc_type_c(cfio_proc_type)

end function

integer(4) function cfio_create(path, cmode, ncid)
    implicit none
    character(len=*), intent(in) :: path
    integer(4) :: cmode, ncid, length

    length = len(trim(path))

    call cfio_create_c(trim(path), length, cmode, ncid, cfio_create)

end function

integer(4) function cfio_def_dim(ncid, name, length, dimid)
    implicit none
    integer(4), intent(in) :: ncid
    character(len=*), intent(in) :: name
    integer(4), intent(in) :: length, dimid 
    integer(4) name_length

    name_length = len(trim(name))

    call cfio_def_dim_c(ncid, trim(name), name_length, length, dimid, cfio_def_dim)

end function

integer(4) function cfio_def_var(ncid, name, xtype, ndims, dimids, start, &
	count, varid)
    implicit none
    integer(4), intent(in) :: ncid, xtype, ndims, varid
    character(len=*), intent(in) :: name
    integer(4), dimension(*), intent(in) :: dimids, start, count
    integer(4) name_length

    name_length = len(trim(name))

    call cfio_def_var_c(ncid, trim(name), name_length, xtype, ndims, dimids, &
	start, count, varid, cfio_def_var)

end function

integer function cfio_put_att_str(ncid, varid, name, values)
    implicit none
    integer(4), intent(in) :: ncid, varid
    character(len=*), intent(in) :: name, values
    integer(4) name_length
    
    name_length = len(trim(name))

    call cfio_put_att_c(ncid, varid, trim(name), name_length, cfio_char, len(values), values, &
	cfio_put_att_str)

end function

integer function cfio_put_att_int(ncid, varid, name, values)
    implicit none
    integer(4), intent(in) :: ncid, varid
    character(len=*), intent(in) :: name
    integer(4), intent(in) :: values
    integer(4) name_length
    
    name_length = len(trim(name))

    call cfio_put_att_c(ncid, varid, trim(name), name_length, cfio_int, 1, values, &
	cfio_put_att_int)

end function

integer function cfio_put_att_real(ncid, varid, name, values)
    implicit none
    integer(4), intent(in) :: ncid, varid
    character(len=*), intent(in) :: name
    real(4), intent(in) :: values
    integer(4) name_length
    
    name_length = len(trim(name))

    call cfio_put_att_c(ncid, varid, trim(name), name_length, cfio_float, 1, values, &
	cfio_put_att_real)

end function

integer function cfio_put_att_double(ncid, varid, name, values)
    implicit none
    integer(4), intent(in) :: ncid, varid
    character(len=*), intent(in) :: name
    real(8), intent(in) :: values
    integer(4) name_length
    
    name_length = len(trim(name))

    call cfio_put_att_c(ncid, varid, trim(name), name_length, cfio_double, 1, values, &
	cfio_put_att_double)

end function

integer function cfio_enddef(ncid)
    implicit none
    integer(4), intent(in) :: ncid

    call cfio_enddef_c(ncid, cfio_enddef)

end function

integer function cfio_put_vara_real(ncid, varid, ndims, start, count, fp)
    implicit none
    integer(4), intent(in) :: ncid, varid, ndims
    integer(4), dimension(*), intent(in) :: start, count 
    real(4), dimension(*), intent(in) :: fp

    call cfio_put_vara_float_c(ncid, varid, ndims, start, count, fp, &
	cfio_put_vara_real)

end function

integer function cfio_put_vara_double(ncid, varid, ndims, start, count, fp)
    implicit none
    integer(4), intent(in) :: ncid, varid, ndims
    integer(4), dimension(*), intent(in) :: start, count 
    real(8), dimension(*), intent(in) :: fp

    call cfio_put_vara_double_c(ncid, varid, ndims, start, count, fp, &
	cfio_put_vara_double)

end function

integer function cfio_put_vara_int(ncid, varid, ndims, start, count, fp)
    implicit none
    integer(4), intent(in) :: ncid, varid, ndims
    integer(4), dimension(*), intent(in) :: start, count 
    integer(4), dimension(*), intent(in) :: fp

    call cfio_put_vara_int_c(ncid, varid, ndims, start, count, fp, &
	cfio_put_vara_int)

end function

integer function cfio_io_end()

    call cfio_io_end_c(cfio_io_end)

end function

integer function cfio_close(ncid)
    integer(4), intent(in) :: ncid

    call cfio_close_c(ncid, cfio_close)

end function

end module

