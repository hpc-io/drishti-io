hid_t fileAccessProperty = H5Pcreate(H5P_FILE_ACCESS);
...
H5AC_cache_config_t cache_config;
cache_config.version = H5AC__CURR_CACHE_CONFIG_VERSION;
H5Pget_mdc_config(m_fileAccessProperty, &cache_config);
cache_config.set_initial_size = 1;
cache_config.initial_size = meta_size;
cache_config.evictions_enabled = 0;
cache_config.incr_mode = H5C_incr__off;
cache_config.flash_incr_mode = H5C_flash_incr__off;
cache_config.decr_mode = H5C_decr__off;
H5Pset_mdc_config(fileAccessProperty, &cache_config);