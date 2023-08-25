#!/usr/bin/env python3

import os
import csv
import time
import json

import datetime

import pandas as pd

from rich import print, box
from rich.console import Group
from rich.padding import Padding
from rich.syntax import Syntax
from rich.panel import Panel
from rich.terminal_theme import TerminalTheme
from rich.terminal_theme import MONOKAI

from recorder_utils import RecorderReader
from recorder_utils.build_offset_intervals import build_offset_intervals

from .includes import *


def get_modules(reader):
    func_list = reader.funcs
    ranks = reader.GM.total_ranks
    modules = set()

    for rank in range(ranks):
        for i in range(reader.LMs[rank].total_records):
            record = reader.records[rank][i]
            func_name = func_list[record.func_id]
            if 'MPI_File' in func_name:
                modules.add('MPI-IO')
            elif 'MPI' in func_name:
                modules.add('MPI')
            elif 'H5' in func_name:
                modules.add('H5F')
            else: modules.add('POSIX')

    return modules


def get_accessed_files(reader):
    ranks = reader.GM.total_ranks
    filemap = {}
    for rank in range(ranks):
        filemap.update(reader.LMs[rank].filemap)

    return filemap


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


def handler(args):
    init_console(args)
    validate_thresholds()

    insights_start_time = time.time()

    reader = RecorderReader(args.log_path)
    df_intervals = build_offset_intervals(reader)
    df_posix_records = init_df_posix_recordes(reader)

    modules = get_modules(reader)
    unique_files = get_accessed_files(reader)

    def add_api(row):
        if 'MPI' in row['function']:
            return 'MPIIO'
        elif 'H5' in row['function']:
            return 'H5F'
        else:
            return 'POSIX'

    df_intervals['api'] = df_intervals.apply(add_api, axis=1)

    def add_duration(row):
        return row['end'] - row['start']
    
    df_intervals['duration'] = df_intervals.apply(add_duration, axis=1)
    df_posix_records['duration'] = df_posix_records.apply(add_duration, axis=1)

    #########################################################################################################################################################################

    # Check usage of POSIX, and MPI-IO per file
    total_size_stdio = 0
    total_size_posix = 0
    total_size_mpiio = 0
    total_size = 0

    total_files = len(unique_files)
    total_files_stdio = 0
    total_files_posix = 0
    total_files_mpiio = 0

    for fid in unique_files.keys():
        df_intervals_in_one_file = df_intervals[(df_intervals['file_id'] == fid)]
        df_stdio_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'STDIO')]
        df_posix_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'POSIX')]
        df_mpiio_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'MPIIO')]

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

    if total_size and total_size_stdio / total_size > THRESHOLD_INTERFACE_STDIO:
        issue = 'Application is using STDIO, a low-performance interface, for {:.2f}% of its data transfers ({})'.format(
            total_size_stdio / total_size * 100.0,
            convert_bytes(total_size_stdio)
        )

        recommendation = [
            {
                'message': 'Consider switching to a high-performance I/O interface such as MPI-IO'
            }
        ]

        insights_operation.append(
            message(args, INSIGHTS_STDIO_HIGH_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )

    if 'MPI-IO' not in modules:
        issue = 'Application is using low-performance interface'

        recommendation = [
            {
                'message': 'Consider switching to a high-performance I/O interface such as MPI-IO'
            }
        ]

        insights_operation.append(
            message(args, INSIGHTS_MPI_IO_NO_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
        )

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
        if total_writes > total_reads and total_operations and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is write operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
                total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
            )

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        if total_reads > total_writes and total_operations and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is read operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
                total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
            )

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_READ_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        total_read_size = df_posix[(df_posix['function'].str.contains('read'))]['size'].sum()
        total_written_size = df_posix[~(df_posix['function'].str.contains('read'))]['size'].sum()

        total_size = total_written_size + total_read_size

        if total_written_size > total_read_size and abs(total_written_size - total_read_size) / total_size > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is write size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
                total_written_size / total_size * 100.0, total_read_size / total_size * 100.0
            )

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        if total_read_size > total_written_size and abs(total_written_size - total_read_size) / total_size > THRESHOLD_OPERATION_IMBALANCE:
            issue = 'Application is read size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
                total_written_size / total_size * 100.0, total_read_size / total_size * 100.0
            )

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_READ_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
            )

        #########################################################################################################################################################################

        # Get the number of small I/O operations (less than 1 MB)

        total_reads_small = len(df_posix[(df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
        total_writes_small = len(df_posix[~(df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])

        detected_files = [] # [fname, num of read, num of write]
        for fid in unique_files.keys():
            read_cnt = len(df_posix[(df_posix['file_id'] == fid) & (df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            write_cnt = len(df_posix[(df_posix['file_id'] == fid) & ~(df_posix['function'].str.contains('read')) & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            detected_files.append([unique_files[fid], read_cnt, write_cnt])

        if total_reads_small and total_reads_small / total_reads > THRESHOLD_SMALL_REQUESTS and total_reads_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
            issue = 'Application issues a high number ({}) of small read requests (i.e., < 1MB) which represents {:.2f}% of all read requests'.format(
                total_reads_small, total_reads_small / total_reads * 100.0
            )
        
            detail = []
            recommendation = []

            for file in detected_files:
                if file[1] > (total_reads * THRESHOLD_SMALL_REQUESTS / 2):
                    detail.append(
                        {
                            'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                                file[1],
                                file[1] / total_reads * 100.0,
                                file[0] if args.full_path else os.path.basename(file[0])
                            ) 
                        }
                    )

            recommendation.append(
                {
                    'message': 'Consider buffering read operations into larger more contiguous ones'
                }
            )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since the appplication already uses MPI-IO, consider using collective I/O calls (e.g. MPI_File_read_all() or MPI_File_read_at_all()) to aggregate requests into larger ones',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default')
                    }
                )
            else:
                recommendation.append(
                    {
                        'message': 'Application does not use MPI-IO for operations, consider use this interface instead to harness collective operations'
                    }
                )

            insights_operation.append(
                message(args, INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        if total_writes_small and total_writes_small / total_writes > THRESHOLD_SMALL_REQUESTS and total_writes_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
            issue = 'Application issues a high number ({}) of small write requests (i.e., < 1MB) which represents {:.2f}% of all write requests'.format(
                total_writes_small, total_writes_small / total_writes * 100.0
            )

            detail = []
            recommendation = []

            for file in detected_files:
                if file[2] > (total_writes * THRESHOLD_SMALL_REQUESTS / 2):
                    detail.append(
                        {
                            'message': '{} ({:.2f}%) small write requests are to "{}"'.format(
                                file[2],
                                file[2] / total_writes * 100.0,
                                file[0] if args.full_path else os.path.basename(file[0])
                            ) 
                        }
                    )

            recommendation.append(
                {
                    'message': 'Consider buffering write operations into larger more contiguous ones'
                }
            )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since the application already uses MPI-IO, consider using collective I/O calls (e.g. MPI_File_write_all() or MPI_File_write_at_all()) to aggregate requests into larger ones',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default')
                    }
                )
            else:
                recommendation.append(
                    {
                        'message': 'Application does not use MPI-IO for operations, consider use this interface instead to harness collective operations'
                    }
                )

            insights_operation.append(
                message(args, INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        #########################################################################################################################################################################

        # How many requests are misaligned?
        # TODO: 

        #########################################################################################################################################################################

        # Redundant read-traffic (based on Phill)
        # POSIX_MAX_BYTE_READ (Highest offset in the file that was read)
        max_read_offset = df_posix[(df_posix['function'].str.contains('read'))]['offset'].max()

        if max_read_offset > total_read_size:
            issue = 'Application might have redundant read traffic (more data read than the highest offset)'

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_REDUNDANT_READ_USAGE, TARGET_DEVELOPER, WARN, issue, None)
            )

        max_write_offset = df_posix[~(df_posix['function'].str.contains('read'))]['offset'].max()

        if max_write_offset > total_written_size:
            issue = 'Application might have redundant write traffic (more data written than the highest offset)'

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, None)
            )

        #########################################################################################################################################################################

        # Check for a lot of random operations

        grp_posix_by_fid = df_posix.groupby('file_id')

        read_consecutive = 0
        read_sequential = 0
        read_random = 0

        for fid, df_filtered in grp_posix_by_fid:
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

        if total_reads:
            if read_random and read_random / total_reads > THRESHOLD_RANDOM_OPERATIONS and read_random > THRESHOLD_RANDOM_OPERATIONS_ABSOLUTE:
                issue = 'Application is issuing a high number ({}) of random read operations ({:.2f}%)'.format(
                    read_random, read_random / total_reads * 100.0
                )

                recommendation = [
                    {
                        'message': 'Consider changing your data model to have consecutive or sequential reads'
                    }
                ]

                insights_operation.append(
                    message(args, INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
                )
            else:
                issue = 'Application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) read requests'.format(
                    read_consecutive / total_reads * 100.0,
                    read_sequential / total_reads * 100.0
                )

                insights_operation.append(
                    message(args, INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE, TARGET_DEVELOPER, OK, issue, None)
                )

        write_consecutive = 0
        write_sequential = 0
        write_random = 0

        for fid, df_filtered in grp_posix_by_fid:
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

        if total_writes:
            if write_random and write_random / total_writes > THRESHOLD_RANDOM_OPERATIONS and write_random > THRESHOLD_RANDOM_OPERATIONS_ABSOLUTE:
                issue = 'Application is issuing a high number ({}) of random write operations ({:.2f}%)'.format(
                    write_random, write_random / total_writes * 100.0
                )

                recommendation = [
                    {
                        'message': 'Consider changing your data model to have consecutive or sequential writes'
                    }
                ]

                insights_operation.append(
                    message(args, INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
                )
            else:
                issue = 'Application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) write requests'.format(
                    write_consecutive / total_writes * 100.0,
                    write_sequential / total_writes * 100.0
                )

                insights_operation.append(
                    message(args, INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE, TARGET_DEVELOPER, OK, issue, None)
                )

        #########################################################################################################################################################################

        # Shared file with small operations

        # A file is shared if it's been read/written by more than 1 rank
        detected_files = grp_posix_by_fid['rank'].nunique()
        shared_files = set(detected_files[detected_files > 1].index)

        total_shared_reads = 0
        total_shared_reads_small = 0
        total_shared_writes = 0
        total_shared_writes_small = 0

        detected_files = [] # [fname, num of read, num of write]
        for fid in shared_files:
            total_shared_reads += len(df_posix[(df_posix['file_id'] == fid) & (df_posix['function'].str.contains('read'))])
            total_shared_writes += len(df_posix[(df_posix['file_id'] == fid) & ~(df_posix['function'].str.contains('read'))])

            read_cnt = len(df_posix[(df_posix['file_id'] == fid) 
                                    & (df_posix['function'].str.contains('read')) 
                                    & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            write_cnt = len(df_posix[(df_posix['file_id'] == fid) 
                                    & ~(df_posix['function'].str.contains('read')) 
                                    & (df_posix['size'] < THRESHOLD_SMALL_BYTES)])
            detected_files.append([unique_files[fid], read_cnt, write_cnt])

            total_shared_reads_small += read_cnt
            total_shared_writes_small += write_cnt

        if total_shared_reads and total_shared_reads_small / total_shared_reads > THRESHOLD_SMALL_REQUESTS and total_shared_reads_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
            issue = 'Application issues a high number ({}) of small read requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file read requests'.format(
                total_shared_reads_small, total_shared_reads_small / total_shared_reads * 100.0
            )

            detail = []

            for file in detected_files:
                if file[1] > (total_reads * THRESHOLD_SMALL_REQUESTS / 2):
                    detail.append(
                        {
                            'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                                file[1],
                                file[1] / total_reads * 100.0,
                                file[0] if args.full_path else os.path.basename(file[0])
                            ) 
                        }
                    )
            
            recommendation = [
                {
                    'message': 'Consider coalesceing read requests into larger more contiguous ones using MPI-IO collective operations',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default')
                }
            ]
        
            insights_operation.append(
                message(args, INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        if total_shared_writes and total_shared_writes_small / total_shared_writes > THRESHOLD_SMALL_REQUESTS and total_shared_writes_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
            issue = 'Application issues a high number ({}) of small write requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file write requests'.format(
                total_shared_writes_small, total_shared_writes_small / total_shared_writes * 100.0
            )

            detail = []

            for file in detected_files:
                if file[2] > (total_writes * THRESHOLD_SMALL_REQUESTS / 2):
                    detail.append(
                        {
                            'message': '{} ({:.2f}%) small write requests are to "{}"'.format(
                                file[2],
                                file[2] / total_writes * 100.0,
                                file[0] if args.full_path else os.path.basename(file[0])
                            ) 
                        }
                    )

            recommendation = [
                {
                    'message': 'Consider coalescing write requests into larger more contiguous ones using MPI-IO collective operations',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(args, INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        #########################################################################################################################################################################

        # TODO: Here I assume all operations other than write/read are metadata operations
        df_posix_metadata = df_posix_records[~(df_posix_records['function'].str.contains('read|write|print', na=False))]
        df_detected = df_posix_metadata.groupby('rank')['duration'].sum().reset_index()
        has_long_metadata = df_detected[(df_detected['duration'] > THRESHOLD_METADATA_TIME_RANK)]

        if not has_long_metadata.empty:
            issue = 'There are {} ranks where metadata operations take over {} seconds'.format(
                len(has_long_metadata), THRESHOLD_METADATA_TIME_RANK
            )

            recommendation = [
                {
                    'message': 'Attempt to combine files, reduce, or cache metadata operations'
                }
            ]

            if 'H5F' in modules:
                recommendation.append(
                    {
                        'message': 'Since your appplication uses HDF5, try enabling collective metadata calls with H5Pset_coll_metadata_write() and H5Pset_all_coll_metadata_ops()',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-collective-metadata.c'), line_numbers=True, background_color='default')
                    },
                    {
                        'message': 'Since your appplication uses HDF5, try using metadata cache to defer metadata operations',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-cache.c'), line_numbers=True, background_color='default')
                    }
                )

            insights_metadata.append(
                message(args, INSIGHTS_POSIX_HIGH_METADATA_TIME, TARGET_DEVELOPER, HIGH, issue, recommendation)
            )

        # We already have a single line for each shared-file access
        # To check for stragglers, we can check the difference between the 

        # POSIX_FASTEST_RANK_BYTES
        # POSIX_SLOWEST_RANK_BYTES
        # POSIX_VARIANCE_RANK_BYTES

        stragglers_count = 0
        
        detected_files = []

        for fid in shared_files:
            df_posix_in_one_file = df_posix[(df_posix['file_id'] == fid)]
            total_transfer_size = df_posix_in_one_file['size'].sum()

            df_detected = df_posix_in_one_file.groupby('rank').agg({'size': 'sum', 'duration': 'sum'}).reset_index()
            slowest_rank_bytes = df_detected.loc[df_detected['duration'].idxmax(), 'size']
            fastest_rank_bytes = df_detected.loc[df_detected['duration'].idxmin(), 'size']

            if total_transfer_size and abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size > THRESHOLD_STRAGGLERS:
                stragglers_count += 1

                detected_files.append([
                    unique_files[fid], abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size * 100
                ])

        if stragglers_count:
            issue = 'Detected data transfer imbalance caused by stragglers when accessing {} shared file.'.format(
                stragglers_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file[0] if args.full_path else os.path.basename(file[0])
                        ) 
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better balancing the data transfer between the application ranks'
                },
                {
                    'message': 'Consider tuning how your data is distributed in the file system by changing the stripe size and count',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(args, INSIGHTS_POSIX_SIZE_IMBALANCE, TARGET_USER, HIGH, issue, recommendation, detail)
            )
    
        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME

        stragglers_count = 0
        
        detected_files = []

        for fid in shared_files:
            df_posix_in_one_file = df_posix[(df_posix['file_id'] == fid)]
            total_transfer_time = df_posix_in_one_file['duration'].sum()

            df_detected = df_posix_in_one_file.groupby('rank')['duration'].sum().reset_index()

            slowest_rank_time = df_detected['duration'].max()
            fastest_rank_time = df_detected['duration'].min()

            if total_transfer_time and abs(slowest_rank_time - fastest_rank_time) / total_transfer_time > THRESHOLD_STRAGGLERS:
                stragglers_count += 1

                detected_files.append([
                    unique_files[fid], abs(slowest_rank_time - fastest_rank_time) / total_transfer_time * 100
                ])

        if stragglers_count:
            issue = 'Detected time imbalance caused by stragglers when accessing {} shared file.'.format(
                stragglers_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file[0] if args.full_path else os.path.basename(file[0])
                        ) 
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better distributing the data in the parallel file system' # needs to review what suggestion to give
                },
                {
                    'message': 'Consider tuning how your data is distributed in the file system by changing the stripe size and count',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(args, INSIGHTS_POSIX_TIME_IMBALANCE, TARGET_USER, HIGH, issue, recommendation, detail)
            )

        # Get the individual files responsible for imbalance
        imbalance_count = 0

        detected_files = []

        for fid in unique_files.keys():
            if fid in shared_files: continue
            df_detected = df_posix[(df_posix['file_id'] == fid) & ~(df_posix['function'].str.contains('read'))]
            
            max_bytes_written = df_detected['size'].max()
            min_bytes_written = df_detected['size'].min()

            if max_bytes_written and abs(max_bytes_written - min_bytes_written) / max_bytes_written > THRESHOLD_IMBALANCE:
                imbalance_count += 1

                detected_files.append([
                    unique_files[fid], abs(max_bytes_written - min_bytes_written) / max_bytes_written  * 100
                ])

        if imbalance_count:
            issue = 'Detected write imbalance when accessing {} individual files'.format(
                imbalance_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file[0] if args.full_path else os.path.basename(file[0])
                        ) 
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better balancing the data transfer between the application ranks'
                },
                {
                    'message': 'Consider tuning the stripe size and count to better distribute the data',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default')
                },
                {
                    'message': 'If the application uses netCDF and HDF5 double-check the need to set NO_FILL values',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/pnetcdf-hdf5-no-fill.c'), line_numbers=True, background_color='default')
                },
                {
                    'message': 'If rank 0 is the only one opening the file, consider using MPI-IO collectives'
                }
            ]

            insights_operation.append(
                message(args, INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

        imbalance_count = 0

        detected_files = []

        for fid in shared_files:
            df_detected = df_posix[(df_posix['file_id'] == fid) & (df_posix['function'].str.contains('read'))]
            
            max_bytes_read = df_detected['size'].max()
            min_bytes_read = df_detected['size'].min()

            if max_bytes_read and abs(max_bytes_read - min_bytes_read) / max_bytes_read > THRESHOLD_IMBALANCE:
                imbalance_count += 1

                detected_files.append([
                    unique_files[fid], abs(max_bytes_read - min_bytes_read) / max_bytes_read  * 100
                ])        

        if imbalance_count:
            issue = 'Detected read imbalance when accessing {} individual files.'.format(
                imbalance_count
            )

            detail = []
            
            for file in detected_files:
                detail.append(
                    {
                        'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                            file[1],
                            file[0] if args.full_path else os.path.basename(file[0])
                        ) 
                    }
                )

            recommendation = [
                {
                    'message': 'Consider better balancing the data transfer between the application ranks'
                },
                {
                    'message': 'Consider tuning the stripe size and count to better distribute the data',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default')
                },
                {
                    'message': 'If the application uses netCDF and HDF5 double-check the need to set NO_FILL values',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/pnetcdf-hdf5-no-fill.c'), line_numbers=True, background_color='default')
                },
                {
                    'message': 'If rank 0 is the only one opening the file, consider using MPI-IO collectives'
                }
            ]

            insights_operation.append(
                message(args, INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )

    #########################################################################################################################################################################

    if df_intervals['api'].eq('MPIIO').any():
        df_mpiio = df_intervals[(df_intervals['api'] == 'MPIIO')]

        df_mpiio_reads = df_mpiio[(df_mpiio['function'].str.contains('read'))]
        mpiio_indp_reads = len(df_mpiio_reads[~(df_mpiio_reads['function'].str.contains('_all'))])
        mpiio_coll_reads = len(df_mpiio_reads[(df_mpiio_reads['function'].str.contains('_all'))])
        total_mpiio_read_operations = mpiio_indp_reads + mpiio_coll_reads

        df_mpiio_writes = df_mpiio[~(df_mpiio['function'].str.contains('read'))]
        mpiio_indp_writes = len(df_mpiio_writes[~(df_mpiio_writes['function'].str.contains('_all'))])
        mpiio_coll_writes = len(df_mpiio_writes[(df_mpiio_writes['function'].str.contains('_all'))])
        total_mpiio_write_operations = mpiio_indp_writes + mpiio_coll_writes

        detected_files = [] # [fname, total_read, total_write]
        for fid in unique_files.keys():
            read_cnt = len(df_mpiio_reads[(df_mpiio_reads['file_id'] == fid) & (df_mpiio_reads['function'].str.contains('read'))])
            write_cnt = len(df_mpiio_reads[(df_mpiio_reads['file_id'] == fid) & ~(df_mpiio_reads['function'].str.contains('read'))])
            detected_files.append([unique_files[fid], read_cnt, write_cnt])

        if mpiio_coll_reads == 0:
            if total_mpiio_read_operations and total_mpiio_read_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                issue = 'Application uses MPI-IO but it does not use collective read operations, instead it issues {} ({:.2f}%) independent read calls'.format(
                    mpiio_indp_reads,
                    mpiio_indp_reads / (total_mpiio_read_operations) * 100
                )

                detail = []

                for file in detected_files:
                    total_cnt = file[1] + file[2]
                    if total_cnt and file[1] / total_cnt > THRESHOLD_COLLECTIVE_OPERATIONS and total_cnt > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                        detail.append(
                            {
                                'message': '{} ({}%) of independent reads to "{}"'.format(
                                    file[1],
                                    file[1] / total_cnt * 100,
                                    file[0] if args.full_path else os.path.basename(file[0])
                                ) 
                            }
                        )

                recommendation = [
                    {
                        'message': 'Use collective read operations (e.g. MPI_File_read_all() or MPI_File_read_at_all()) and set one aggregator per compute node',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default')
                    }
                ]

                insights_operation.append(
                    message(args, INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
                )
        else:
            issue = 'Application uses MPI-IO and read data using {} ({:.2f}%) collective operations'.format(
                mpiio_coll_reads,
                mpiio_coll_reads / total_mpiio_read_operations * 100
            )

            insights_operation.append(
                message(args, INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE, TARGET_DEVELOPER, OK, issue)
            )

        if mpiio_coll_writes == 0:
            if total_mpiio_write_operations and total_mpiio_write_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                issue = 'Application uses MPI-IO but it does not use collective write operations, instead it issues {} ({:.2f}%) independent write calls'.format(
                    mpiio_indp_writes,
                    mpiio_indp_writes / (total_mpiio_write_operations) * 100
                )

                detail = []

                for file in detected_files:
                    total_cnt = file[1] + file[2]
                    if total_cnt and file[2] / total_cnt > THRESHOLD_COLLECTIVE_OPERATIONS and total_cnt > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
                        detail.append(
                            {
                                'message': '{} ({}%) of independent writes to "{}"'.format(
                                    file[2],
                                    file[2] / total_cnt * 100,
                                    file[0] if args.full_path else os.path.basename(file[0])
                                ) 
                            }
                        )

                recommendation = [
                    {
                        'message': 'Use collective write operations (e.g. MPI_File_write_all() or MPI_File_write_at_all()) and set one aggregator per compute node',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default')
                    }
                ]

                insights_operation.append(
                    message(args, INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
                )

        else:
            issue = 'Application uses MPI-IO and write data using {} ({:.2f}%) collective operations'.format(
                mpiio_coll_writes,
                mpiio_coll_writes / total_mpiio_write_operations * 100
            )

            insights_operation.append(
                message(args, INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE, TARGET_DEVELOPER, OK, issue)
            )

        #########################################################################################################################################################################

        # Look for usage of non-block operations

        # Look for HDF5 file extension

        has_hdf5_extension = False

        for fid in unique_files.keys():
            fname = unique_files[fid]
            if fname.endswith('.h5') or fname.endswith('.hdf5'):
                has_hdf5_extension = True

        if len(df_mpiio_reads[(df_mpiio_reads['function'].str.contains('iread|begin|end'))]) == 0:
            issue = 'Application could benefit from non-blocking (asynchronous) reads'

            recommendation = []

            if 'H5F' in modules or has_hdf5_extension:
                recommendation.append(
                    {
                        'message': 'Since you use HDF5, consider using the ASYNC I/O VOL connector (https://github.com/hpc-io/vol-async)',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-vol-async-read.c'), line_numbers=True, background_color='default')
                    }
                )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since you use MPI-IO, consider non-blocking/asynchronous I/O operations', # (e.g., MPI_File_iread(), MPI_File_read_all_begin/end(), or MPI_File_read_at_all_begin/end())',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-iread.c'), line_numbers=True, background_color='default')
                    }
                )

            insights_operation.append(
                message(args, INSIGHTS_MPI_IO_BLOCKING_READ_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
            )

        if len(df_mpiio_writes[(df_mpiio_writes['function'].str.contains('iwrite|begin|end'))]) == 0:
            issue = 'Application could benefit from non-blocking (asynchronous) writes'

            recommendation = []

            if 'H5F' in modules or has_hdf5_extension:
                recommendation.append(
                    {
                        'message': 'Since you use HDF5, consider using the ASYNC I/O VOL connector (https://github.com/hpc-io/vol-async)',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-vol-async-write.c'), line_numbers=True, background_color='default')
                    }
                )

            if 'MPI-IO' in modules:
                recommendation.append(
                    {
                        'message': 'Since you use MPI-IO, consider non-blocking/asynchronous I/O operations',  # (e.g., MPI_File_iwrite(), MPI_File_write_all_begin/end(), or MPI_File_write_at_all_begin/end())',
                        'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-iwrite.c'), line_numbers=True, background_color='default')
                    }
                )

            insights_operation.append(
                message(args, INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
            )

    #########################################################################################################################################################################

    # Nodes and MPI-IO aggregators
    # If the application uses collective reads or collective writes, look for the number of aggregators
    # TODO:

    #########################################################################################################################################################################
    
    NUMBER_OF_COMPUTE_NODES = 0

    #########################################################################################################################################################################

    codes = []
    if args.json:
        f = open(args.json)
        data = json.load(f)

        for key, values in data.items():
            for value in values:
                code = value['code']
                codes.append(code)

                level = value['level']
                issue = value['issue']
                recommendation = []
                for rec in value['recommendations']:
                    new_message = {'message': rec}
                    recommendation.append(new_message)

                insights_dxt.append(
                    message(args, code, TARGET_DEVELOPER, level, issue, recommendation)
                )

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
                ' [b]COMPUTE NODES[/b]   [white]{}[/white]'.format(
                    NUMBER_OF_COMPUTE_NODES
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

    if insights_metadata:
        console.print(
            Panel(
                Padding(
                    Group(
                        *insights_metadata
                    ),
                    (1, 1)
                ),
                title='METADATA',
                title_align='left'
            )
        )

    if insights_operation:
        console.print(
            Panel(
                Padding(
                    Group(
                        *insights_operation
                    ),
                    (1, 1)
                ),
                title='OPERATIONS',
                title_align='left'
            )
        )

    if insights_dxt:
        console.print(
            Panel(
                Padding(
                    Group(
                        *insights_dxt
                    ),
                    (1, 1)
                ),
                title='DXT',
                title_align='left'
            )
        )
        
    console.print(
        Panel(
            ' {} | [white]LBNL[/white] | [white]Drishti report generated at {} in[/white] {:.3f} seconds'.format(
                datetime.datetime.now().year,
                datetime.datetime.now(),
                insights_end_time - insights_start_time
            ),
            box=box.SIMPLE
        )
    )

    if args.export_theme_light:
        export_theme = TerminalTheme(
            (255, 255, 255),
            (0, 0, 0),
            [
                (26, 26, 26),
                (244, 0, 95),
                (152, 224, 36),
                (253, 151, 31),
                (157, 101, 255),
                (244, 0, 95),
                (88, 209, 235),
                (120, 120, 120),
                (98, 94, 76),
            ],
            [
                (244, 0, 95),
                (152, 224, 36),
                (224, 213, 97),
                (157, 101, 255),
                (244, 0, 95),
                (88, 209, 235),
                (246, 246, 239),
            ],
        )
    else:
        export_theme = MONOKAI

    if args.export_html:
        console.save_html(
            '{}.html'.format(args.log_path),
            theme=export_theme,
            clear=False
        )

    if args.export_svg:
        console.save_svg(
            '{}.svg'.format(args.log_path),
            title='Drishti',
            theme=export_theme,
            clear=False
        )

    if args.export_csv:
        issues = [
            'JOB',
            INSIGHTS_STDIO_HIGH_USAGE,
            INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE,
            INSIGHTS_POSIX_READ_COUNT_INTENSIVE,
            INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE,
            INSIGHTS_POSIX_READ_SIZE_INTENSIVE,
            INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE,
            INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE,
            INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE,
            INSIGHTS_POSIX_HIGH_MISALIGNED_FILE_USAGE,
            INSIGHTS_POSIX_REDUNDANT_READ_USAGE,
            INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE,
            INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE,
            INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE,
            INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE,
            INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE,
            INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE,
            INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE,
            INSIGHTS_POSIX_HIGH_METADATA_TIME,
            INSIGHTS_POSIX_SIZE_IMBALANCE,
            INSIGHTS_POSIX_TIME_IMBALANCE,
            INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE,
            INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE,
            INSIGHTS_MPI_IO_NO_USAGE,
            INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE,
            INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE,
            INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE,
            INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE,
            INSIGHTS_MPI_IO_BLOCKING_READ_USAGE,
            INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE,
            INSIGHTS_MPI_IO_AGGREGATORS_INTRA,
            INSIGHTS_MPI_IO_AGGREGATORS_INTER,
            INSIGHTS_MPI_IO_AGGREGATORS_OK
        ]
        if codes:
            issues.extend(codes)

        detected_issues = dict.fromkeys(issues, False)
        detected_issues['JOB'] = None

        for report in csv_report:
            detected_issues[report] = True

        filename = '{}-summary.csv'.format(
            args.log_path
        )

        with open(filename, 'w') as f:
            w = csv.writer(f)
            w.writerow(detected_issues.keys())
            w.writerow(detected_issues.values())


