hid_t fileAccessProperty = H5Pcreate(H5P_FILE_ACCESS);
...
H5Pset_all_coll_metadata_ops(fileAccessProperty, true);
H5Pset_coll_metadata_write(fileAccessProperty, true);