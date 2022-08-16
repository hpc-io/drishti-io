# ------------------------------- #
# MPICH                           #
# ------------------------------- #
export MPICH_MPIIO_HINTS="*:cb_nodes=16:cb_buffer_size=16777216:romio_cb_write=enable:romio_ds_write=disable:romio_cb_read=enable:romio_ds_read=disable"

# * means it will apply the hints to any file opened with MPI-IO
# cb_nodes        ---> number of aggregator nodes, defaults to stripe count
# cb_buffer_size  ---> controls the buffer size used for collective buffering
# romio_cb_write  ---> controls collective buffering for writes 
# romio_cb_read   ---> controls collective buffering for reads
# romio_ds_write  ---> controls data sieving for writes 
# romio_ds_read   ---> controls data sieving for reads

# to visualize the used hints for a given job
export MPICH_MPIIO_HINTS_DISPLAY=1

# ------------------------------- #
# OpenMPI / SpectrumMPI (Summit)  #
# ------------------------------- #
export OMPI_MCA_io=romio321
export ROMIO_HINTS=./my-romio-hints

# the my-romio-hints file content is as follows:
cat $ROMIO_HINTS

romio_cb_write enable
romio_cb_read  enable
romio_ds_write disable
romio_ds_read  disable
cb_buffer_size 16777216
cb_nodes 8