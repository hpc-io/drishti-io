hid_t es_id, fid, gid, did;

MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);

es_id = H5EScreate();                         // Create event set for tracking async operations
fid = H5Fopen_async(..., es_id);              // Asynchronous, can start immediately
gid = H5Gopen_async(fid, ..., es_id);         // Asynchronous, starts when H5Fopen completes
did = H5Dopen_async(gid, ..., es_id);         // Asynchronous, starts when H5Gopen completes

status = H5Dwrite_async(did, ..., es_id);     // Asynchronous, starts when H5Dopen completes

// Wait for operations in event set to complete, buffers used for H5Dwrite must only be changed after
H5ESwait(es_id, H5ES_WAIT_FOREVER, &num_in_progress, &op_failed); 

H5ESclose(es_id);                             // Close the event set (must wait first)