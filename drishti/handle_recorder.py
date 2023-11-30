#!/usr/bin/env python3

import os
import time
import pandas as pd
from recorder_utils import RecorderReader
from recorder_utils.build_offset_intervals import build_offset_intervals
from .module import *


def get_accessed_files(reader):
    ranks = reader.GM.total_ranks
    file_map = {}
    for rank in range(ranks):
        file_map.update(reader.LMs[rank].filemap)
    return file_map


def init_df_posix_recordes(reader):
    func_list = reader.funcs
    ranks = reader.GM.total_ranks
    records = []
    for rank in range(ranks):
        for i in range(reader.LMs[rank].total_records):
            record = reader.records[rank][i]
            func_name = func_list[record.func_id]

            if 'MPI' not in func_name and 'H5' not in func_name:
                records.append( [rank, func_name, record.tstart, record.tend] )

    head = ['rank', 'function', 'start', 'end']
    df_posix_records = pd.DataFrame(records, columns=head)
    return df_posix_records


def handler():
    init_console()
    validate_thresholds()

    insights_start_time = time.time()

    reader = RecorderReader(args.log_path)
    df_intervals = build_offset_intervals(reader)
    df_posix_records = init_df_posix_recordes(reader)

    file_map = get_accessed_files(reader)

    def add_api(row):
        if 'MPI' in row['function']:
            return 'MPI-IO'
        elif 'H5' in row['function']:
            return 'H5F'
        else:
            return 'POSIX'

    def add_duration(row):
        return row['end'] - row['start']
    
    df_intervals['api'] = df_intervals.apply(add_api, axis=1)
    df_intervals['duration'] = df_intervals.apply(add_duration, axis=1)
    df_posix_records['duration'] = df_posix_records.apply(add_duration, axis=1)

    modules = set(df_intervals['api'].unique())

    #########################################################################################################################################################################

    # Check usage of POSIX, and MPI-IO per file
    total_size_stdio = 0
    total_size_posix = 0
    total_size_mpiio = 0
    total_size = 0

    total_files = len(file_map)
    total_files_stdio = 0
    total_files_posix = 0
    total_files_mpiio = 0

    for id in file_map.keys():
        df_intervals_in_one_file = df_intervals[(df_intervals['file_id'] == id)]
        df_stdio_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'STDIO')]
        df_posix_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'POSIX')]
        df_mpiio_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'MPI-IO')]

        if len(df_stdio_intervals_in_one_file):
            total_files_stdio += 1
            total_size_stdio += df_stdio_intervals_in_one_file['size'].sum()

        if len(df_posix_intervals_in_one_file):
            total_files_posix += 1
            total_size_posix += df_posix_intervals_in_one_file['size'].sum()

        if len(df_mpiio_intervals_in_one_file):
            total_files_mpiio += 1
            total_size_mpiio += df_mpiio_intervals_in_one_file['size'].sum()       


    # Since POSIX will capture both POSIX-only accesses and those comming from MPI-IO, we can subtract those
    if total_size_posix > 0 and total_size_posix >= total_size_mpiio:
        total_size_posix -= total_size_mpiio

    total_size = total_size_stdio + total_size_posix + total_size_mpiio

    assert(total_size_stdio >= 0)
    assert(total_size_posix >= 0)
    assert(total_size_mpiio >= 0)

    check_stdio(total_size, total_size_stdio)
    check_mpiio(modules)

    #########################################################################################################################################################################

    if df_intervals['api'].eq('POSIX').any():
        df_posix = df_intervals[(df_intervals['api'] == 'POSIX')]

        #########################################################################################################################################################################

        # Get number of write/read operations
        total_reads = len(df_posix[(df_posix['function'].str.contains('read'))])
        total_writes = len(df_posix[~(df_posix['function'].str.contains('read'))])

        # Get total number of I/O operations
        total_operations = total_writes + total_reads 

        # To check whether the application is write-intersive or read-intensive we only look at the POSIX level and check if the difference between reads and writes is larger than 10% (for more or less), otherwise we assume a balance
        check_operation_intensive(total_operations, total_reads, total_writes)

        total_read_size = df_posix[(df_posix['function'].str.contains('read'))]['size'].sum()
        total_written_size = df_posix[~(df_posix['function'].str.contains('read'))]['size'].sum()

        total_size = total_written_size + total_read_size

        check_size_intensive(total_size, total_read_size, total_written_size)

        #########################################################################################################################################################################

        # Get the number of small I/O operations (less than 1 MB)

        total_reads_small = len(df_posix[(df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
        total_writes_small = len(df_posix[~(df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])

        detected_files = []
        for id in file_map.keys():
            read_cnt = len(df_posix[(df_posix['file_id'] == id) & (df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            write_cnt = len(df_posix[(df_posix['file_id'] == id) & ~(df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            detected_files.append([id, read_cnt, write_cnt])

        column_names = ['id', 'total_reads', 'total_writes']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules, file_map)

        #########################################################################################################################################################################

        # How many requests are misaligned?
        # TODO: 

        #########################################################################################################################################################################

        # Redundant read-traffic (based on Phill)
        # POSIX_MAX_BYTE_READ (Highest offset in the file that was read)
        max_read_offset = df_posix[(df_posix['function'].str.contains('read'))]['offset'].max()
        max_write_offset = df_posix[~(df_posix['function'].str.contains('read'))]['offset'].max()
        
        check_traffic(max_read_offset, total_read_size, max_write_offset, total_written_size)

        #########################################################################################################################################################################

        # Check for a lot of random operations

        grp_posix_by_id = df_posix.groupby('file_id')

        read_consecutive = 0
        read_sequential = 0
        read_random = 0

        for id, df_filtered in grp_posix_by_id:
            df_filtered = df_filtered[(df_filtered['function'].str.contains('read'))].sort_values('start')

            for i in range(len(df_filtered) - 1):
                curr_interval = df_filtered.iloc[i]
                next_interval = df_filtered.iloc[i + 1]
                if curr_interval['offset'] + curr_interval['size'] == next_interval['offset']:
                    read_consecutive += 1
                elif curr_interval['offset'] + curr_interval['size'] < next_interval['offset']:
                    read_sequential += 1
                else:
                    read_random += 1

        write_consecutive = 0
        write_sequential = 0
        write_random = 0

        for id, df_filtered in grp_posix_by_id:
            df_filtered = df_filtered[~(df_filtered['function'].str.contains('read'))].sort_values('start')

            for i in range(len(df_filtered) - 1):
                curr_interval = df_filtered.iloc[i]
                next_interval = df_filtered.iloc[i + 1]
                if curr_interval['offset'] + curr_interval['size'] == next_interval['offset']:
                    write_consecutive += 1
                elif curr_interval['offset'] + curr_interval['size'] < next_interval['offset']:
                    write_sequential += 1
                else:
                    write_random += 1

        check_random_operation(read_consecutive, read_sequential, read_random, total_reads, write_consecutive, write_sequential, write_random, total_writes)

        #########################################################################################################################################################################

        # Shared file with small operations

        # A file is shared if it's been read/written by more than 1 rank
        detected_files = grp_posix_by_id['rank'].nunique()
        shared_files = set(detected_files[detected_files > 1].index)

        total_shared_reads = 0
        total_shared_reads_small = 0
        total_shared_writes = 0
        total_shared_writes_small = 0

        detected_files = []
        for id in shared_files:
            total_shared_reads += len(df_posix[(df_posix['file_id'] == id) & (df_posix['function'].str.contains('read'))])
            total_shared_writes += len(df_posix[(df_posix['file_id'] == id) & ~(df_posix['function'].str.contains('read'))])

            read_cnt = len(df_posix[(df_posix['file_id'] == id) 
                                    & (df_posix['function'].str.contains('read')) 
                                    & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            write_cnt = len(df_posix[(df_posix['file_id'] == id) 
                                    & ~(df_posix['function'].str.contains('read')) 
                                    & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            detected_files.append([id, read_cnt, write_cnt])

            total_shared_reads_small += read_cnt
            total_shared_writes_small += write_cnt
        
        column_names = ['id', 'INSIGHTS_POSIX_SMALL_READS', 'INSIGHTS_POSIX_SMALL_WRITES']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes, total_shared_writes_small, detected_files, file_map)

        #########################################################################################################################################################################

        # TODO: Here I assume all operations other than write/read are metadata operations
        df_posix_metadata = df_posix_records[~(df_posix_records['function'].str.contains('read|write|print', na=False))]
        df_detected = df_posix_metadata.groupby('rank')['duration'].sum().reset_index()
        count_long_metadata = len(df_detected[(df_detected['duration'] > THRESHOLD_METADATA_TIME_RANK)])

        check_long_metadata(count_long_metadata, modules)
  
        # We already have a single line for each shared-file access
        # To check for stragglers, we can check the difference between the 

        # POSIX_FASTEST_RANK_BYTES
        # POSIX_SLOWEST_RANK_BYTES
        # POSIX_VARIANCE_RANK_BYTES

        stragglers_count = 0
        
        detected_files = []
        for id in shared_files:
            df_posix_in_one_file = df_posix[(df_posix['file_id'] == id)]
            total_transfer_size = df_posix_in_one_file['size'].sum()

            df_detected = df_posix_in_one_file.groupby('rank').agg({'size': 'sum', 'duration': 'sum'}).reset_index()
            slowest_rank_bytes = df_detected.loc[df_detected['duration'].idxmax(), 'size']
            fastest_rank_bytes = df_detected.loc[df_detected['duration'].idxmin(), 'size']

            if total_transfer_size and abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size > THRESHOLD_STRAGGLERS:
                stragglers_count += 1

                detected_files.append([
                    id, abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size * 100
                ])
        
        column_names = ['id', 'data_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_shared_data_imblance(stragglers_count, detected_files, file_map)
    
        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME

        stragglers_count = 0
        
        detected_files = []
        for id in shared_files:
            df_posix_in_one_file = df_posix[(df_posix['file_id'] == id)]
            total_transfer_time = df_posix_in_one_file['duration'].sum()

            df_detected = df_posix_in_one_file.groupby('rank')['duration'].sum().reset_index()

            slowest_rank_time = df_detected['duration'].max()
            fastest_rank_time = df_detected['duration'].min()

            if total_transfer_time and abs(slowest_rank_time - fastest_rank_time) / total_transfer_time > THRESHOLD_STRAGGLERS:
                stragglers_count += 1

                detected_files.append([
                    id, abs(slowest_rank_time - fastest_rank_time) / total_transfer_time * 100
                ])

        column_names = ['id', 'time_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_shared_time_imbalance(stragglers_count, detected_files, file_map)

        # Get the individual files responsible for imbalance
        imbalance_count = 0

        detected_files = []
        for id in file_map.keys():
            if id in shared_files: continue
            df_detected = df_posix[(df_posix['file_id'] == id) & ~(df_posix['function'].str.contains('read'))]
            
            max_bytes_written = df_detected['size'].max()
            min_bytes_written = df_detected['size'].min()

            if max_bytes_written and abs(max_bytes_written - min_bytes_written) / max_bytes_written > THRESHOLD_IMBALANCE:
                imbalance_count += 1

                detected_files.append([
                    id, abs(max_bytes_written - min_bytes_written) / max_bytes_written  * 100
                ])

        column_names = ['id', 'write_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_individual_write_imbalance(imbalance_count, detected_files, file_map)

        imbalance_count = 0

        detected_files = []
        for id in shared_files:
            df_detected = df_posix[(df_posix['file_id'] == id) & (df_posix['function'].str.contains('read'))]
            
            max_bytes_read = df_detected['size'].max()
            min_bytes_read = df_detected['size'].min()

            if max_bytes_read and abs(max_bytes_read - min_bytes_read) / max_bytes_read > THRESHOLD_IMBALANCE:
                imbalance_count += 1

                detected_files.append([
                    id, abs(max_bytes_read - min_bytes_read) / max_bytes_read  * 100
                ])

        column_names = ['id', 'read_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_individual_read_imbalance(imbalance_count, detected_files, file_map)

    #########################################################################################################################################################################

    if df_intervals['api'].eq('MPI-IO').any():
        df_mpiio = df_intervals[(df_intervals['api'] == 'MPI-IO')]

        df_mpiio_reads = df_mpiio[(df_mpiio['function'].str.contains('read'))]
        mpiio_indep_reads = len(df_mpiio_reads[~(df_mpiio_reads['function'].str.contains('_all'))])
        mpiio_coll_reads = len(df_mpiio_reads[(df_mpiio_reads['function'].str.contains('_all'))])
        total_mpiio_read_operations = mpiio_indep_reads + mpiio_coll_reads

        df_mpiio_writes = df_mpiio[~(df_mpiio['function'].str.contains('read'))]
        mpiio_indep_writes = len(df_mpiio_writes[~(df_mpiio_writes['function'].str.contains('_all'))])
        mpiio_coll_writes = len(df_mpiio_writes[(df_mpiio_writes['function'].str.contains('_all'))])
        total_mpiio_write_operations = mpiio_indep_writes + mpiio_coll_writes

        detected_files = []
        if mpiio_coll_reads == 0 and total_mpiio_read_operations and total_mpiio_read_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
            for id in file_map.keys():
                indep_read_count = df_mpiio_reads[~(df_mpiio_reads['function'].str.contains('_all')) & (df_mpiio_reads['file_id'] == id)]
                indep_write_count = df_mpiio_writes[~(df_mpiio_writes['function'].str.contains('_all')) & (df_mpiio_writes['file_id'] == id)]
                indep_total_count = indep_read_count + indep_write_count;

                if (indep_total_count > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE and indep_read_count / indep_total_count > THRESHOLD_COLLECTIVE_OPERATIONS):
                    detected_files.append([
                        id, indep_read_count, indep_read_count / indep_total_count * 100
                    ])

        column_names = ['id', 'absolute_indep_reads', 'percent_indep_reads']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_mpi_collective_read_operation(mpiio_coll_reads, mpiio_indep_reads, total_mpiio_read_operations, detected_files, file_map)

        detected_files = []
        if mpiio_coll_writes == 0 and total_mpiio_write_operations and total_mpiio_write_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
            for id in file_map.keys():
                indep_read_count = df_mpiio_reads[~(df_mpiio_reads['function'].str.contains('_all')) & (df_mpiio_reads['file_id'] == id)]
                indep_write_count = df_mpiio_writes[~(df_mpiio_writes['function'].str.contains('_all')) & (df_mpiio_writes['file_id'] == id)]
                indep_total_count = indep_read_count + indep_write_count;

                if (indep_total_count > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE and indep_write_count / indep_total_count > THRESHOLD_COLLECTIVE_OPERATIONS):
                    detected_files.append([
                        id, indep_write_count, indep_write_count / indep_total_count * 100
                    ])

        column_names = ['id', 'absolute_indep_writes', 'percent_indep_writes']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_mpi_collective_write_operation(mpiio_coll_writes, mpiio_indep_writes, total_mpiio_write_operations, detected_files, file_map)

        #########################################################################################################################################################################

        # Look for usage of non-block operations

        # Look for HDF5 file extension

        has_hdf5_extension = False

        for id in file_map.keys():
            fname = file_map[id]
            if fname.endswith('.h5') or fname.endswith('.hdf5'):
                has_hdf5_extension = True

        mpiio_nb_reads = len(df_mpiio_reads[(df_mpiio_reads['function'].str.contains('iread|begin|end'))])
        mpiio_nb_writes = len(df_mpiio_writes[(df_mpiio_writes['function'].str.contains('iwrite|begin|end'))])

        check_mpi_none_block_operation(mpiio_nb_reads, mpiio_nb_writes, has_hdf5_extension, modules)

    #########################################################################################################################################################################

    # Nodes and MPI-IO aggregators
    # If the application uses collective reads or collective writes, look for the number of aggregators
    # TODO:

    #########################################################################################################################################################################

    insights_end_time = time.time()

    console.print()

    console.print(
        Panel(
            '\n'.join([
                ' [b]RECORDER[/b]:       [white]{}[/white]'.format(
                    os.path.basename(args.log_path)
                ),
                ' [b]FILES[/b]:          [white]{} files ({} use STDIO, {} use POSIX, {} use MPI-IO)[/white]'.format(
                    total_files,
                    total_files_stdio,
                    total_files_posix - total_files_mpiio,  # Since MPI-IO files will always use POSIX, we can decrement to get a unique count
                    total_files_mpiio
                ),
                ' [b]PROCESSES[/b]       [white]{}[/white]'.format(
                    reader.GM.total_ranks
                ),
            ]),
            title='[b][slate_blue3]DRISHTI[/slate_blue3] v.0.5[/b]',
            title_align='left',
            subtitle='[red][b]{} critical issues[/b][/red], [orange1][b]{} warnings[/b][/orange1], and [white][b]{} recommendations[/b][/white]'.format(
                insights_total[HIGH],
                insights_total[WARN],
                insights_total[RECOMMENDATIONS],
            ),
            subtitle_align='left',
            padding=1
        )
    )

    console.print()

    display_content()
    display_footer(insights_start_time, insights_end_time)

    export_html()
    export_svg()

    filename = '{}-summary.csv'.format(
        args.log_path
    )
    export_csv(filename)

