status = nc_def_var (ncid, "A", NC_DOUBLE, 3, cube_dim, &cube1_id);
nc_def_var_fill(ncid, cube1_id, NC_NOFILL, NULL);

status = nc_def_var (ncid, "B", NC_DOUBLE, 3, cube_dim, &cube2_id);
nc_def_var_fill(ncid, cube1_id, NC_NOFILL, NULL);

status = nc_def_var (ncid, "C", NC_DOUBLE, 3, cube_dim, &cube3_id);
nc_def_var_fill(ncid, cube1_id, NC_NOFILL, NULL);