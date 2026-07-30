#!/usr/bin/env python3

import datetime
import csv
import time
import pandas as pd
from rich import box
from rich.syntax import Syntax
from drishti.includes.config import *

'''
Before calling the functions below
Make sure the variables passed are in the given structure:
file_map: a dict of (id, path) pair
modules: a set or a dict should be ok
detected_files: A pandas dataframe
'''

# Basic usage check

def check_stdio(total_size, total_size_stdio):
    '''
    Check whether the application has excessively utilized standard input/output operations

    Parameters:
        total_size: total I/O size
        total_size_stdio: total STDIO size
    
    '''

    if total_size and total_size_stdio / total_size > thresholds['interface_stdio'][0]:
        thresholds['interface_stdio'][1] = True
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
            message(INSIGHTS_STDIO_HIGH_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )


def check_mpiio(modules):
    '''
    Check whether the application has used MPI-IO or not

    Parameter:
        modules: all different mudules been used in the application
    '''

    if 'MPI-IO' not in modules:
        issue = 'Application is using low-performance interface'

        recommendation = [
            {
                'message': 'Consider switching to a high-performance I/O interface such as MPI-IO'
            }
        ]

        insights_operation.append(
            message(INSIGHTS_MPI_IO_NO_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
        )



# POSIX level check


def check_operation_intensive(total_operations, total_reads, total_writes):
    '''
    Check whether the application is read or write intensive

    Parameters:
        total_operations: number of I/O operations been executed by the application
        total_reads: number of read operations been executed by the application
        total_writes: number of write operations been executed by the application
    '''

    if total_writes > total_reads and total_operations and abs(total_writes - total_reads) / total_operations > thresholds['imbalance_operations'][0]:
        issue = 'Application is write operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
            total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )

    if total_reads > total_writes and total_operations and abs(total_writes - total_reads) / total_operations > thresholds['imbalance_operations'][0]:
        issue = 'Application is read operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
            total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_READ_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )


def check_size_intensive(total_size, total_read_size, total_written_size):
    '''
    Check whether the application is read size intensive or written size intensive

    Parameters:
        total_size: Total I/O size measured in byte
        total_read_size: Input I/O size measured in byte
        total_written_size: Output I/O size measured in byte
    '''

    if total_written_size > total_read_size and abs(total_written_size - total_read_size) / total_size > thresholds['imbalance_operations'][0]:
        issue = 'Application is write size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
            total_written_size / total_size * 100.0, total_read_size / total_size * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )

    if total_read_size > total_written_size and abs(total_written_size - total_read_size) / total_size > thresholds['imbalance_operations'][0]:
        issue = 'Application is read size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
            total_written_size / total_size * 100.0, total_read_size / total_size * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_READ_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )


def check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules, file_map, dxt_posix=None, dxt_posix_read_data=None, dxt_posix_write_data=None):
    '''
    Check whether application has performed an excessive number of small operations

    Parameters:
        total_reads: number of read operations been executed by the application
        total_reads_small: number of read operations that has small size
        total_writes: number of write operations been executed by the application
        total_writes_small: number of write operations that has small size
        detected_files: 
            total_reads and total_writes in each file
            required columns: ['id', 'total_reads', 'total_writes']
        modules: all different mudules been used in the application
        file_map: file id and file name pairing
        df_posix: all POSIX records
    '''

    if total_reads_small and total_reads_small / total_reads > thresholds['small_requests'][0] and total_reads_small > thresholds['small_requests_absolute'][0]:
        thresholds['small_requests_absolute'][1] = True
        issue = 'Application issues a high number ({}) of small read requests (i.e., < 1MB) which represents {:.2f}% of all read requests'.format(
            total_reads_small, total_reads_small / total_reads * 100.0
        )

        detail = []
        recommendation = []
        file_count = 0
        dxt_trigger_time = 0

        for index, row in detected_files.iterrows():
            if row['total_reads'] > (total_reads * thresholds['small_requests'][0] / 2):
                detail.append(
                    {
                        'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                            row['total_reads'],
                            row['total_reads'] / total_reads * 100.0,
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                        ) 
                    }
                )

                # DXT Analysis
                if args.backtrace:
                    start = time.time()
                    if file_count < thresholds['backtrace'][0]:
                        temp = dxt_posix.loc[dxt_posix['id'] == int(row['id'])]
                        temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == int(row['id'])]

                        if not temp_df.empty: 
                            temp_df = temp_df.loc[temp_df['length'] < thresholds['small_requests'][0]]
                            small_read_requests_ranks = temp_df['rank'].unique()
                            if len(small_read_requests_ranks) > 0:  
                                if len(small_read_requests_ranks) > 1 and int(small_read_requests_ranks[0]) == 0:
                                    rank_df = temp.loc[(temp['rank'] == int(small_read_requests_ranks[1]))]
                                else:
                                    rank_df = temp.loc[(temp['rank'] == int(small_read_requests_ranks[0]))]
                            
                                rank_df = rank_df['read_segments'].iloc[0]
                                rank_addresses = rank_df['stack_memory_addresses'].iloc[0]
                                address = dxt_posix.iloc[0]['address_line_mapping']['address']
                                res = set(list(address)) & set(rank_addresses)
                                backtrace = dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                        
                        if len(small_read_requests_ranks) > 0:
                            detail.append(
                                {
                                    'message': '{} rank(s) made small read requests in "{}". Below is the backtrace information:'.format(
                                        len(small_read_requests_ranks),
                                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                                    ) 
                                }
                            )
                            
                            for index, row in backtrace.iterrows():
                                detail.append(
                                    {
                                        'message': '{}: {}'.format(
                                            row['function_name'],
                                            row['line_number']
                                        ) 
                                    }
                                )
                        file_count += 1
                    else:
                        detail.append(
                            {
                                'message': 'The backtrace information for this file is similar to the previous files'
                            }
                        )

                    end = time.time()
                    time_taken = end - start
                    dxt_trigger_time += time_taken

        if dxt_trigger_time > 0:            
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
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
            message(INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )

    if total_writes_small and total_writes_small / total_writes > thresholds['small_requests'][0] and total_writes_small > thresholds['small_requests_absolute'][0]:
        thresholds['small_requests_absolute'][1] = True
        issue = 'Application issues a high number ({}) of small write requests (i.e., < 1MB) which represents {:.2f}% of all write requests'.format(
            total_writes_small, total_writes_small / total_writes * 100.0
        )

        dxt_trigger_time = 0

        detail = []
        recommendation = []
        file_count = 0
        for index, row in detected_files.iterrows():
            if row['total_writes'] > (total_writes * thresholds['small_requests'][0] / 2):
                detail.append(
                    {
                        'message': '{} ({:.2f}%) small write requests are to "{}"'.format(
                            row['total_writes'],
                            row['total_writes'] / total_writes * 100.0,
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                        ) 
                    }
                )

                # DXT Analysis
                if args.backtrace:
                    start = time.time()
                    if file_count < thresholds['backtrace'][0]:
                        temp = dxt_posix.loc[dxt_posix['id'] == int(row['id'])]
                        temp_df = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == int(row['id'])]

                        if not temp_df.empty: 
                            temp_df = temp_df.loc[temp_df['length'] < thresholds['small_requests'][0]]
                            small_write_requests_ranks = temp_df['rank'].unique()   
                            if len(small_write_requests_ranks) > 0:
                                if int(small_write_requests_ranks[0]) == 0 and len(small_write_requests_ranks) > 1:
                                    rank_df = temp.loc[(temp['rank'] == int(small_write_requests_ranks[1]))]
                                else:
                                    rank_df = temp.loc[(temp['rank'] == int(small_write_requests_ranks[0]))] 
                                
                                rank_df = temp.loc[(temp['rank'] == int(small_write_requests_ranks[0]))]
                                rank_df = rank_df['write_segments'].iloc[0]
                                rank_addresses = rank_df['stack_memory_addresses'].iloc[0]
                                address = dxt_posix.iloc[0]['address_line_mapping']['address']
                                res = set(list(address)) & set(rank_addresses)
                                backtrace = dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                            
                        if len(small_write_requests_ranks) > 0:
                            detail.append(
                                {
                                    'message': '{} rank(s) made small write requests in "{}". Below is the backtrace information:'.format(
                                        len(small_write_requests_ranks),
                                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                                    ) 
                                }
                            )
                            
                            for index, row in backtrace.iterrows():
                                detail.append(
                                    {
                                        'message': '{}: {}'.format(
                                            row['function_name'],
                                            row['line_number']
                                        ) 
                                    }
                                )
                        
                        file_count += 1
                    else:
                        detail.append(
                            {
                                'message': 'The backtrace information for this file is similar to previous files'
                            }
                        )

                    end = time.time()
                    time_taken = end - start
                    dxt_trigger_time += time_taken
        
        if dxt_trigger_time > 0:
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
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
            message(INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )


def check_misaligned(total_operations, total_mem_not_aligned, total_file_not_aligned, modules, file_map=None, df_lustre=None, dxt_posix=None, dxt_posix_read_data=None):
    '''
    Check whether application has excessive misaligned operations

    Parameters:
        total_operations: number of I/O operations been executed by the application
        total_mem_not_aligned: number of memory requests not aligned
        total_file_not_aligned: number of file requests not aligned
        modules: all different mudules been used in the application
    '''

    if total_operations and total_mem_not_aligned / total_operations > thresholds['misaligned_requests'][0]:
        thresholds['misaligned_requests'][1] = True
        issue = 'Application has a high number ({:.2f}%) of misaligned memory requests'.format(
            total_mem_not_aligned / total_operations * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE, TARGET_DEVELOPER, HIGH, issue, None)
        )

    if total_operations and total_file_not_aligned / total_operations > thresholds['misaligned_requests'][0]:
        thresholds['misaligned_requests'][1] = True
        issue = 'Application issues a high number ({:.2f}%) of misaligned file requests'.format(
            total_file_not_aligned / total_operations * 100.0
        )

        recommendation = [
            {
                'message': 'Consider aligning the requests to the file system block boundaries'
            }
        ]

        if 'HF5' in modules:
            recommendation.append(
                {
                    'message': 'Since the appplication uses HDF5, consider using H5Pset_alignment() in a file access property list',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-alignment.c'), line_numbers=True, background_color='default')
                },
                {
                    'message': 'Any file object greater than or equal in size to threshold bytes will be aligned on an address which is a multiple of alignment'
                }
            )

        detail = []
        if 'LUSTRE' in modules:
            # DXT Analysis
            if args.backtrace:
                start = time.time()
                
                if not df_lustre['counters']['LUSTRE_STRIPE_SIZE'].empty:
                    stripe_size = df_lustre['counters']['LUSTRE_STRIPE_SIZE'].iloc[0]
                else:
                    stripe_size = df_lustre['counters']['POSIX_FILE_ALIGNMENT'].iloc[0]

                file_count = 0

                ids = dxt_posix.id.unique().tolist()
                for id in ids:
                    temp = dxt_posix.loc[dxt_posix['id'] == id]
                    temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == id]

                    misaligned_ranks = []
                    misaligned_ranks_opr = []
                    
                    offsets = temp_df["offsets"].to_numpy().tolist()
                    rank = temp_df["rank"].to_numpy().tolist()
                    operation = temp_df["operation"].to_numpy().tolist()

                    for i in range(len(offsets)):
                        if offsets[i] % stripe_size != 0:
                            misaligned_ranks.append(rank[i])
                            misaligned_ranks_opr.append(operation[i])

                    if misaligned_ranks:
                        misaligned_rank_ind = misaligned_ranks[0]
                        misaligned_rank_opr = misaligned_ranks_opr[0]
                        misaligned_rank_df = temp.loc[(temp['rank'] == int(misaligned_rank_ind))]
                        if misaligned_rank_opr == 'read':
                            misaligned_rank_df = misaligned_rank_df['read_segments'].iloc[0]
                        else:
                            misaligned_rank_df = misaligned_rank_df['write_segments'].iloc[0]
                        misaligned_rank_stack_addresses = misaligned_rank_df['stack_memory_addresses'].iloc[0]

                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(misaligned_rank_stack_addresses)
                        backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]

                        detail.append(
                            {
                                'message': '{} rank(s) made misaligned requests in "{}". Below is the backtrace information:'.format(
                                    len(misaligned_ranks),
                                    file_map[id] if args.full_path else os.path.basename(file_map[id])
                                ) 
                            }
                        )

                        for index, row3 in backtrace.iterrows():
                            detail.append(
                                {
                                    'message': '{}: {}'.format(
                                        row3['function_name'],
                                        row3['line_number']
                                    ) 
                                }
                            )
                    file_count += 1

                end = time.time()
                time_taken = end - start
                detail.append(
                    {
                        'message': 'Time taken to process this trigger: {}s'.format(round(time_taken, 5))
                    }
                )
            recommendation.append(
                {
                    'message': 'Consider using a Lustre alignment that matches the file system stripe configuration',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/lustre-striping.bash'), line_numbers=True, background_color='default')
                }
            )

        insights_metadata.append(
            message(INSIGHTS_POSIX_HIGH_MISALIGNED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )


def check_traffic(max_read_offset, total_read_size, max_write_offset, total_written_size, dxt_posix=None, dxt_posix_read_data=None, dxt_posix_write_data=None):
    '''
    Check whether application has redundant read or write traffic

    Parameters:
        max_read_offset: max offset application is reading from
        total_read_size: total size application has been read
        max_write_offset: max offset application is writing to
        total_written_size: total size application has been written
    '''

    if max_read_offset > total_read_size:
        issue = 'Application might have redundant read traffic (more data read than the highest offset)'

        detail = []
        file_count = 0

        # DXT Analysis
        if args.backtrace:
            start = time.time()
            ids = dxt_posix.id.unique().tolist()
            for id in ids:
                if file_count < thresholds['backtrace'][0]:
                    temp = dxt_posix.loc[dxt_posix['id'] == id]

                    random_ranks_ind = -1
                    temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == id]
                    updated_offsets = (temp_df["offsets"].to_numpy()).tolist()

                    for i in range(len(updated_offsets)):
                        if updated_offsets.count(updated_offsets[i]) > 1: 
                            redundant_ranks_ind = i
                            break

                    if random_ranks_ind != -1:
                        random_rank = temp_df.iloc[redundant_ranks_ind]['rank']
                        random_offsets = temp_df.iloc[redundant_ranks_ind]['offsets']
                        random_start_time = temp_df.iloc[random_ranks_ind]['start_time']

                        temp_random_rank = temp.loc[(temp['rank'] == int(random_rank))]
                        temp_random_rank = temp_random_rank['read_segments'].iloc[0]
                        random_stack_addresses = temp_random_rank.loc[(temp_random_rank['offset'] == random_offsets) & (temp_random_rank['start_time'] == random_start_time)]
                        random_stack_addresses = random_stack_addresses['stack_memory_addresses'].iloc[0]

                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(random_stack_addresses)
                        backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                        
                        detail.append(
                            {
                                'message': 'The backtrace information for these redundant read call(s) is given below:'
                            }
                        )
                        for index, row3 in backtrace.iterrows():
                            detail.append(
                                {
                                    'message': '{}: {}'.format(
                                        row3['function_name'],
                                        row3['line_number']
                                    ) 
                                }
                            )
                        file_count += 1
                else:
                    detail.append(
                        {
                            'message': 'The backtrace information for this file is similar to the previous files'
                        }
                    )
            end = time.time()
            time_taken = end - start
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(time_taken, 5))
                }
            )

        insights_metadata.append(
            message(INSIGHTS_POSIX_REDUNDANT_READ_USAGE, TARGET_DEVELOPER, WARN, issue, None)
        )

    if max_write_offset > total_written_size:
        issue = 'Application might have redundant write traffic (more data written than the highest offset)'

        detail = []
        file_count = 0

        # DXT Analysis
        if args.backtrace:
            start = time.time()
            ids = dxt_posix.id.unique().tolist()
            for id in ids:
                if file_count < thresholds['backtrace'][0]:
                    temp = dxt_posix.loc[dxt_posix['id'] == id]

                    random_ranks_ind = -1
                    temp_df = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == id]
                    updated_offsets = (temp_df["offsets"].to_numpy()).tolist()
                    for i in range(len(updated_offsets)):
                        if updated_offsets.count(updated_offsets[i]) > 1: 
                            redundant_ranks_ind = i
                            break

                    if random_ranks_ind != -1:
                        random_rank = temp_df.iloc[redundant_ranks_ind]['rank']
                        random_offsets = temp_df.iloc[redundant_ranks_ind]['offsets']
                        random_start_time = temp_df.iloc[random_ranks_ind]['start_time']

                        temp_random_rank = temp.loc[(temp['rank'] == int(random_rank))]
                        temp_random_rank = temp_random_rank['write_segments'].iloc[0]
                        random_stack_addresses = temp_random_rank.loc[(temp_random_rank['offset'] == random_offsets) & (temp_random_rank['start_time'] == random_start_time)]
                        random_stack_addresses = random_stack_addresses['stack_memory_addresses'].iloc[0]

                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(random_stack_addresses)
                        backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                        
                        detail.append(
                            {
                                'message': 'The backtrace information for these redundant write call(s) is given below:'
                            }
                        )
                        for index, row3 in backtrace.iterrows():
                            detail.append(
                                {
                                    'message': '{}: {}'.format(
                                        row3['function_name'],
                                        row3['line_number']
                                    ) 
                                }
                            )
                        file_count += 1
                else:
                    detail.append(
                        {
                            'message': 'The backtrace information for this file is similar to the previous files'
                        }
                    )
            end = time.time()
            time_taken = end - start
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(time_taken, 5))
                }
            )
        insights_metadata.append(
            message(INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, None, detail)
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, None)
        )


def check_random_operation(read_consecutive, read_sequential, read_random, total_reads, write_consecutive, write_sequential, write_random, total_writes, dxt_posix=None, dxt_posix_read_data=None, dxt_posix_write_data=None):
    '''
    Check whether application has performed excessive random operations

    Parameters:
        read_consecutive: number of consecutive read operations
        read_sequential: number of sequential read operations
        read_random: number of random read operations
        total_read: number of read operations been executed by the application
        write_consecutive: number of consecutive write operations
        write_sequential: number of sequential write operations
        write_random: number of random write operations
        total_write: number of write operations been executed by the application
    '''

    if total_reads:
        if read_random and read_random / total_reads > thresholds['random_operations'][0] and read_random > thresholds['random_operations_absolute'][0]:
            thresholds['random_operations'][1] = True
            thresholds['random_operations_absolute'][1] = True
            issue = 'Application is issuing a high number ({}) of random read operations ({:.2f}%)'.format(
                read_random, read_random / total_reads * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider changing your data model to have consecutive or sequential reads'
                }
            ]

            # DXT Analysis
            if args.backtrace:
                start = time.time()
                ids = dxt_posix.id.unique().tolist()
                for id in ids:
                    temp = dxt_posix.loc[dxt_posix['id'] == id]
                    temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == id]
                    temp_df = temp_df.sort_values('start_time', ascending=True)
                    random_ranks_ind = -1
                
                    if not temp_df["offsets"].is_monotonic_increasing:
                        updated_offsets = (temp_df["offsets"].to_numpy()).tolist()
                        cur = 0
                        for i in range(len(updated_offsets)):
                            if updated_offsets[i] < cur:
                                random_ranks_ind = i
                                break
                            cur = updated_offsets[i]

                    if random_ranks_ind != -1:
                        random_rank = temp_df.iloc[random_ranks_ind]['rank']
                        random_offsets = temp_df.iloc[random_ranks_ind]['offsets']
                        random_start_time = temp_df.iloc[random_ranks_ind]['start_time']
                        temp_random_rank = temp.loc[(temp['rank'] == int(random_rank))]
                        temp_random_rank = temp_random_rank['read_segments'].iloc[0]
                        random_stack_addresses = temp_random_rank.loc[(temp_random_rank['offset'] == random_offsets) & (temp_random_rank['start_time'] == random_start_time)]
                        random_stack_addresses = random_stack_addresses['stack_memory_addresses'].iloc[0]

                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(random_stack_addresses)
                        backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                        detail = []
                        detail.append(
                            {
                                'message': 'The backtrace information for these random read call(s) is given below:'
                            }
                        )
                        for index, row3 in backtrace.iterrows():
                            detail.append(
                                {
                                    'message': '{}: {}'.format(
                                        row3['function_name'],
                                        row3['line_number']
                                    ) 
                                }
                            )
                end = time.time()
                time_taken = end - start
                detail.append(
                    {
                        'message': 'Time taken to process this trigger: {}s'.format(round(time_taken, 5))
                    }
                )

            insights_operation.append(
                message(INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
            )
        else:
            issue = 'Application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) read requests'.format(
                read_consecutive / total_reads * 100.0,
                read_sequential / total_reads * 100.0
            )

            insights_operation.append(
                message(INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE, TARGET_DEVELOPER, OK, issue, None)
            )

    if total_writes:
        if write_random and write_random / total_writes > thresholds['random_operations'][0] and write_random > thresholds['random_operations_absolute'][0]:
            thresholds['random_operations'][1] = True
            thresholds['random_operations_absolute'][1] = True
            issue = 'Application is issuing a high number ({}) of random write operations ({:.2f}%)'.format(
                write_random, write_random / total_writes * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider changing your data model to have consecutive or sequential writes'
                }
            ]

            # DXT Analysis
            if args.backtrace:
                start = time.time()
                ids = dxt_posix.id.unique().tolist()
                for id in ids:
                    temp = dxt_posix.loc[dxt_posix['id'] == id]

                    temp_df = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == id]
                    temp_df.sort_values('start_time', ascending=True, inplace=True)
                    random_ranks_ind = -1
                    if not temp_df["offsets"].is_monotonic_increasing:
                        updated_offsets = (temp_df["offsets"].to_numpy()).tolist()
                        cur = 0
                        for i in range(len(updated_offsets)):
                            if updated_offsets[i] < cur:
                                random_ranks_ind = i
                                break
                            cur = updated_offsets[i]

                    if random_ranks_ind != -1:
                        random_rank = temp_df.iloc[random_ranks_ind]['rank']
                        random_offsets = temp_df.iloc[random_ranks_ind]['offsets']
                        random_start_time = temp_df.iloc[random_ranks_ind]['start_time']
                        
                        temp_random_rank = temp.loc[(temp['rank'] == int(random_rank))]
                        temp_random_rank = temp_random_rank['write_segments'].iloc[0]
                        random_stack_addresses = temp_random_rank.loc[(temp_random_rank['offset'] == random_offsets) & (temp_random_rank['start_time'] == random_start_time)]
                        random_stack_addresses = random_stack_addresses['stack_memory_addresses'].iloc[0]

                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(random_stack_addresses)
                        backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                        detail = []
                        detail.append(
                            {
                                'message': 'The backtrace information for these random write call(s) is given below:'
                            }
                        )
                        for index, row3 in backtrace.iterrows():
                            detail.append(
                                {
                                    'message': '{}: {}'.format(
                                        row3['function_name'],
                                        row3['line_number']
                                    ) 
                                }
                            )
                
                end = time.time()
                time_taken = end - start
                detail.append(
                    {
                        'message': 'Time taken to process this trigger: {}s'.format(round(time_taken, 5))
                    }
                )

            insights_operation.append(
                message(INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation)
            )
        else:
            issue = 'Application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) write requests'.format(
                write_consecutive / total_writes * 100.0,
                write_sequential / total_writes * 100.0
            )

            insights_operation.append(
                message(INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE, TARGET_DEVELOPER, OK, issue, None)
            )


def check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes, total_shared_writes_small, shared_files, file_map):
    '''
    Check whether there are excessive small requests in shared files

    Parameters:
        total_shared_reads: total read operations in shared files
        total_shared_reads_small: small read operations in shared files
        total_shared_writes: total write operations in shared files
        total_shared_writes_small: small write operations in shared files
        shared_files:
            small reads an small writes in each shared file
            required columns: ['id', 'INSIGHTS_POSIX_SMALL_READS', 'INSIGHTS_POSIX_SMALL_WRITES']
        file_map: file id and file name pairing
    '''

    if total_shared_reads and total_shared_reads_small / total_shared_reads > thresholds['small_requests'][0] and total_shared_reads_small > thresholds['small_requests_absolute'][0]:
        thresholds['small_requests'][1] = True
        thresholds['small_requests_absolute'][1] = True
        issue = 'Application issues a high number ({}) of small read requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file read requests'.format(
            total_shared_reads_small, total_shared_reads_small / total_shared_reads * 100.0
        )

        detail = []

        for index, row in shared_files.iterrows():
            if row['INSIGHTS_POSIX_SMALL_READS'] > (total_shared_reads * thresholds['small_requests'][0] / 2):
                detail.append(
                    {
                        'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                            row['INSIGHTS_POSIX_SMALL_READS'],
                            row['INSIGHTS_POSIX_SMALL_READS'] / total_shared_reads * 100.0,
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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
            message(INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )

    if total_shared_writes and total_shared_writes_small / total_shared_writes > thresholds['small_requests'][0] and total_shared_writes_small > thresholds['small_requests_absolute'][0]:
        thresholds['small_requests'][1] = True
        thresholds['small_requests_absolute'][1] = True
        issue = 'Application issues a high number ({}) of small write requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file write requests'.format(
            total_shared_writes_small, total_shared_writes_small / total_shared_writes * 100.0
        )

        detail = []

        for index, row in shared_files.iterrows():
            if row['INSIGHTS_POSIX_SMALL_WRITES'] > (total_shared_writes * thresholds['small_requests'][0] / 2):
                detail.append(
                    {
                        'message': '{} ({:.2f}%) small writes requests are to "{}"'.format(
                            row['INSIGHTS_POSIX_SMALL_WRITES'],
                            row['INSIGHTS_POSIX_SMALL_WRITES'] / total_shared_writes * 100.0,
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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
            message(INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )


def check_long_metadata(count_long_metadata, modules):
    '''
    Check how many ranks have metadata operations taking too long

    Parameters:
        count_long_metadata: number of ranks that have metadata operations taking too long
        modules: all different mudules been used in the application
    '''

    if count_long_metadata > 0:
        thresholds['metadata_time_rank'][1] = True
        issue = 'There are {} ranks where metadata operations take over {} seconds'.format(
            count_long_metadata, thresholds['metadata_time_rank'][0]
        )

        recommendation = [
            {
                'message': 'Attempt to combine files, reduce, or cache metadata operations'
            }
        ]

        if 'HF5' in modules:
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
            message(INSIGHTS_POSIX_HIGH_METADATA_TIME, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )


def check_shared_data_imblance(stragglers_count, detected_files, file_map, dxt_posix=None, dxt_posix_read_data=None, dxt_posix_write_data=None):
    '''
    Check how many shared files containing data transfer imbalance

    Parameters:
        stragglers_count: number of shared files that contain data transfer imbalane
        detected_files:
            data imbalance per file
            required columns: ['id', 'data_imbalance']
        file_map: file id and file name pairing
    '''

    if stragglers_count:
        thresholds['imbalance_stragglers'][1] = True
        issue = 'Detected data transfer imbalance caused by stragglers when accessing {} shared file.'.format(
            stragglers_count
        )

        detail = []
        file_count = 0
        dxt_trigger_time = 0

        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['data_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                    ) 
                }
            )

            # DXT Analysis
            if args.backtrace:
                start = time.time()
                if file_count < thresholds['backtrace'][0]:
                    temp = dxt_posix.loc[dxt_posix['id'] == int(row['id'])]
                    temp_df_1 = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == int(row['id'])]
                    temp_df_2 = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == int(row['id'])]

                    df_merged = pd.concat([temp_df_1, temp_df_2], ignore_index=True, sort=False)
                    df_merged['duration'] = df_merged['end_time'] - df_merged['start_time']
                    df_merged.sort_values('duration', ascending=True, inplace=True)
                    df_merged = df_merged.iloc[0]
                    rank_df = temp.loc[(temp['rank'] == int(df_merged['rank']))]

                    if df_merged['operation'] == 'write':
                        rank_df = rank_df['write_segments'].iloc[0]
                        stack_memory_addresses = rank_df['stack_memory_addresses'].iloc[0]
                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(stack_memory_addresses)
                        backtrace = dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]
                    else:
                        rank_df = rank_df['read_segments'].iloc[0]
                        stack_memory_addresses = rank_df['stack_memory_addresses'].iloc[0]
                        address = dxt_posix.iloc[0]['address_line_mapping']['address']
                        res = set(list(address)) & set(stack_memory_addresses)
                        backtrace = dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]

                    detail.append(
                        {
                            'message': 'The backtrace information for these imbalanced call(s) is given below:'
                        }
                    )
                    for index, row3 in backtrace.iterrows():
                        detail.append(
                            {
                                'message': '{}: {}'.format(
                                    row3['function_name'],
                                    row3['line_number']
                                ) 
                            }
                        )

                    file_count += 1
                else:
                    detail.append(
                        {
                            'message': 'The backtrace information for this file is similar to the previous files'
                        }
                    )
                
                end = time.time()
                time_taken = end - start
                dxt_trigger_time += time_taken
        
        if dxt_trigger_time > 0:            
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
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
            message(INSIGHTS_POSIX_SIZE_IMBALANCE, TARGET_USER, HIGH, issue, recommendation, detail)
        )


def check_shared_data_imblance_split(slowest_rank_bytes, fastest_rank_bytes, total_transfer_size):
    '''
    Check whether the specific shared file contains data imbalance

    Parameters:
        slowest_rank_bytes: the total request size of the rank that takes the longest data operation time
        fastest_rank_bytes: the total request size of the rank that takes the shortest data operation time
        total_transfer_size: total request size of that specific shared file
    '''

    if total_transfer_size and abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size > thresholds['imbalance_stragglers'][0]:
        thresholds['imbalance_stragglers'][1] = True
        issue = 'Load imbalance of {:.2f}% detected'.format(
            abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size * 100
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
            message(INSIGHTS_POSIX_SIZE_IMBALANCE, TARGET_USER, HIGH, issue, recommendation)
        )


def check_shared_time_imbalance(stragglers_count, detected_files, file_map):
    '''
    Check how many shared files containing time transfer imbalance

    Parameters:
        stragglers_count: number of shared files that contain time transfer imbalane
        detected_files:
            data imbalance per file
            required columns: ['id', 'time_imbalance']
        file_map: file id and file name pairing
    '''

    if stragglers_count:
        thresholds['imbalance_stragglers'][1] = True
        issue = 'Detected time imbalance caused by stragglers when accessing {} shared file.'.format(
            stragglers_count
        )

        detail = []
        
        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['time_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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
            message(INSIGHTS_POSIX_TIME_IMBALANCE, TARGET_USER, HIGH, issue, recommendation, detail)
        )


def check_shared_time_imbalance_split(slowest_rank_time, fastest_rank_time, total_transfer_time):
    '''
    Check whether the specific shared file contains time imbalance

    Parameters:
        slowest_rank_bytes: the total request time of the rank that takes the longest data operation time
        fastest_rank_bytes: the total request time of the rank that takes the shortest data operation time
        total_transfer_size: total request time of that specific shared file
    '''

    if total_transfer_time and abs(slowest_rank_time - fastest_rank_time) / total_transfer_time > thresholds['imbalance_stragglers'][0]:
        thresholds['imbalance_stragglers'][1] = True
        issue = 'Load imbalance of {:.2f}% detected'.format(
            abs(slowest_rank_time - fastest_rank_time) / total_transfer_time * 100
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
            message(INSIGHTS_POSIX_TIME_IMBALANCE, TARGET_USER, HIGH, issue, recommendation)
        )


def check_individual_write_imbalance(imbalance_count, detected_files, file_map, dxt_posix=None, dxt_posix_write_data=None):
    '''
    Check how many write imbalance when accessing individual files

    Parameters:
        imbalance_count: number of individual files that have write imbalance
        detected_files:
            write imbalance per file
            required columns: ['id', 'write_imbalance']
    '''

    if imbalance_count:
        thresholds['imbalance_size'][1] = True
        issue = 'Detected write imbalance when accessing {} individual files'.format(
            imbalance_count
        )

        detail = []
        file_count = 0
        dxt_trigger_time = 0
        
        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['write_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                    ) 
                }
            )

            # DXT Analysis
            if args.backtrace:
                start = time.time()
                if file_count < thresholds['backtrace'][0]:
                    temp = dxt_posix.loc[dxt_posix['id'] == int(row['id'])]
                    temp_df = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == int(row['id'])]

                    maxClm = temp_df['length'].max()
                    temp_df = temp_df.loc[(temp_df['length'] == maxClm)]
                    rank_df = temp.loc[(temp['rank'] == int(temp_df['rank'].iloc[0]))]

                    rank_df = rank_df['write_segments'].iloc[0]
                    stack_memory_addresses = rank_df['stack_memory_addresses'].iloc[0]
                    address = dxt_posix.iloc[0]['address_line_mapping']['address']
                    res = set(list(address)) & set(stack_memory_addresses)
                    backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]

                    detail.append(
                        {
                            'message': 'The backtrace information for these imbalanced write call(s) is given below:'
                        }
                    )
                    for index, row3 in backtrace.iterrows():
                        detail.append(
                            {
                                'message': '{}: {}'.format(
                                    row3['function_name'],
                                    row3['line_number']
                                ) 
                            }
                        )

                    file_count += 1
                else:
                    detail.append(
                        {
                            'message': 'The backtrace information for this file is similar to the previous files'
                        }
                    )    
                 
                end = time.time()
                time_taken = end - start
                dxt_trigger_time  += time_taken
        
        if dxt_trigger_time > 0:        
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
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
            message(INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )


def check_individual_write_imbalance_split(max_bytes_written, min_bytes_written):
    '''
    Check whether there is write imbalance in the specific individual file

    Parameters:
        max_bytes_written: max byte written in the file
        min_bytes_written: minimum byte written in the file
    '''

    if max_bytes_written and abs(max_bytes_written - min_bytes_written) / max_bytes_written > thresholds['imbalance_size'][0]:
        thresholds['imbalance_size'][1] = True
        issue = 'Load imbalance of {:.2f}% detected'.format(
            abs(max_bytes_written - min_bytes_written) / max_bytes_written  * 100
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
            message(INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )


def check_individual_read_imbalance(imbalance_count, detected_files, file_map, dxt_posix=None, dxt_posix_read_data=None):
    '''
    Check how many read imbalance when accessing individual files

    Parameters:
        imbalance_count: number of individual files that have read imbalance
        detected_files:
            read imbalance per file
            required columns: ['id', 'read_imbalance']
    '''

    if imbalance_count:
        thresholds['imbalance_size'][1] = True
        issue = 'Detected read imbalance when accessing {} individual files.'.format(
            imbalance_count
        )

        detail = []
        file_count = 0
        dxt_trigger_time = 0
        
        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['read_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                    ) 
                }
            )

            # DXT Analysis
            if args.backtrace:
                start = time.time()
                if file_count < thresholds['backtrace'][0]:
                    temp = dxt_posix.loc[dxt_posix['id'] == int(row['id'])]
                    temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == int(row['id'])]

                    maxClm = temp_df['length'].max()
                    temp_df = temp_df.loc[(temp_df['length'] == maxClm)]
                    rank_df = temp.loc[(temp['rank'] == int(temp_df['rank'].iloc[0]))]

                    rank_df = rank_df['read_segments'].iloc[0]
                    stack_memory_addresses = rank_df['stack_memory_addresses'].iloc[0]
                    address = dxt_posix.iloc[0]['address_line_mapping']['address']
                    res = set(list(address)) & set(stack_memory_addresses)
                    backtrace  =  dxt_posix.iloc[0]['address_line_mapping'].loc[dxt_posix.iloc[0]['address_line_mapping']['address'].isin(res)]

                    detail.append(
                        {
                            'message': 'The backtrace information for these imbalanced read call(s) is given below:'
                        }
                    )
                    for index, row3 in backtrace.iterrows():
                        detail.append(
                            {
                                'message': '{}: {}'.format(
                                    row3['function_name'],
                                    row3['line_number']
                                ) 
                            }
                        )

                    file_count += 1
                else:
                    detail.append(
                        {
                            'message': 'The backtrace information for this file is similar to the previous files'
                        }
                    )
                end = time.time()
                time_taken = end - start
                dxt_trigger_time += time_taken

        if dxt_trigger_time > 0:      
            detail.append(
                {
                    'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
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
            message(INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )


def check_individual_read_imbalance_split(max_bytes_read, min_bytes_read):
    '''
    Check whether there is read imbalance in the specific individual file

    Parameters:
        max_bytes_written: max byte read in the file
        min_bytes_written: minimum byte read in the file
    '''

    if max_bytes_read and abs(max_bytes_read - min_bytes_read) / max_bytes_read > thresholds['imbalance_size'][0]:
        thresholds['imbalance_size'][1] = True
        issue = 'Load imbalance of {:.2f}% detected'.format(
            abs(max_bytes_read - min_bytes_read) / max_bytes_read  * 100
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
            message(INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation)
        )


# MPIIO level check


def check_mpi_collective_read_operation(mpiio_coll_reads, mpiio_indep_reads, total_mpiio_read_operations, detected_files, file_map, dxt_mpiio=None):
    '''
    Check whether application uses collective mpi read calls

    Parameters:
        mpiio_coll_reads: number of mpiio read operations that are collective
        mpiio_indep_reads: number of mpiio read operations that are independent
        total_mpiio_read_operations: total mpiio read operations
        detected_files:
            independent read operations and percentage per file
            required columns: ['id', 'absolute_indep_reads', 'percent_indep_reads']
        file_map: file id and file name pairing
    '''

    if mpiio_coll_reads == 0:
        if total_mpiio_read_operations and total_mpiio_read_operations > thresholds['collective_operations_absolute'][0]:
            thresholds['collective_operations_absolute'][1] = True
            issue = 'Application uses MPI-IO but it does not use collective read operations, instead it issues {} ({:.2f}%) independent read calls'.format(
                mpiio_indep_reads,
                mpiio_indep_reads / total_mpiio_read_operations * 100
            )

            detail = []

            dxt_trigger_time = 0

            for index, row in detected_files.iterrows():
                detail.append(
                    {
                        'message': '{} ({}%) of independent reads to "{}"'.format(
                            row['absolute_indep_reads'],
                            row['percent_indep_reads'],
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                        ) 
                    }
                )

                # DXT Analysis
                if args.backtrace:
                    start = time.time()
                    temp = dxt_mpiio.loc[(dxt_mpiio['id'] == int(row['id'])) & (dxt_mpiio['rank'] == 1)]
                    temp = temp['read_segments'].iloc[0]
                    stack_memory_addresses = temp['stack_memory_addresses'].iloc[0]
                    address = dxt_mpiio.iloc[0]['address_line_mapping']['address']
                    res = set(list(address)) & set(stack_memory_addresses)
                    backtrace  =  dxt_mpiio.iloc[0]['address_line_mapping'].loc[dxt_mpiio.iloc[0]['address_line_mapping']['address'].isin(res)]
                    detail.append(
                        {
                            'message': 'The backtrace information for these read call(s) is given below:'
                        }
                    )
                    for index, row3 in backtrace.iterrows():
                        detail.append(
                            {
                                'message': '{}: {}'.format(
                                    row3['function_name'],
                                    row3['line_number']
                                ) 
                            }
                        )
        
                    end = time.time()
                    time_taken = end - start
                    dxt_trigger_time += time_taken
        
            if dxt_trigger_time > 0:            
                detail.append(
                    {
                        'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
                    }
                )

            recommendation = [
                {
                    'message': 'Use collective read operations (e.g. MPI_File_read_all() or MPI_File_read_at_all()) and set one aggregator per compute node',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-read.c'), line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )
    else:
        issue = 'Application uses MPI-IO and read data using {} ({:.2f}%) collective operations'.format(
            mpiio_coll_reads,
            mpiio_coll_reads / total_mpiio_read_operations * 100
        )

        insights_operation.append(
            message(INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE, TARGET_DEVELOPER, OK, issue)
        )


def check_mpi_collective_write_operation(mpiio_coll_writes, mpiio_indep_writes, total_mpiio_write_operations, detected_files, file_map, dxt_mpiio=None):
    '''
    Check whether application uses collective mpi write calls

    Parameters:
        mpiio_coll_writes: number of mpiio write operations that are collective
        mpiio_indep_writes: number of mpiio write operations that are independent
        total_mpiio_write_operations: total mpiio write operations
        detected_files:
            independent write operations and percentage per file
            required columns: ['id', 'absolute_indep_writes', 'percent_indep_writes']
        file_map: file id and file name pairing
    '''

    if mpiio_coll_writes == 0:
        if total_mpiio_write_operations and total_mpiio_write_operations > thresholds['collective_operations_absolute'][0]:
            thresholds['collective_operations_absolute'][1] = True
            issue = 'Application uses MPI-IO but it does not use collective write operations, instead it issues {} ({:.2f}%) independent write calls'.format(
                mpiio_indep_writes,
                mpiio_indep_writes / total_mpiio_write_operations * 100
            )

            detail = []

            dxt_trigger_time = 0

            for index, row in detected_files.iterrows():
                detail.append(
                    {
                        'message': '{} ({}%) independent writes to "{}"'.format(
                            row['absolute_indep_writes'],
                            row['percent_indep_writes'],
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
                        ) 
                    }
                )

                # DXT Analysis
                if args.backtrace:
                    start = time.time()
                    temp = dxt_mpiio.loc[(dxt_mpiio['id'] == int(row['id'])) & (dxt_mpiio['rank'] == 1)]
                    temp = temp['write_segments'].iloc[0]
                    stack_memory_addresses = temp['stack_memory_addresses'].iloc[0]
                    address = dxt_mpiio.iloc[0]['address_line_mapping']['address']
                    res = set(list(address)) & set(stack_memory_addresses)
                    backtrace  =  dxt_mpiio.iloc[0]['address_line_mapping'].loc[dxt_mpiio.iloc[0]['address_line_mapping']['address'].isin(res)]
                    detail.append(
                        {
                            'message': 'The backtrace information for these write call(s) is given below:'
                        }
                    )
                    for index, row3 in backtrace.iterrows():
                        detail.append(
                            {
                                'message': '{}: {}'.format(
                                    row3['function_name'],
                                    row3['line_number']
                                ) 
                            }
                        )

                    end = time.time()
                    time_taken = end - start
                    dxt_trigger_time += time_taken
            
            if dxt_trigger_time > 0:
                detail.append(
                    {
                        'message': 'Time taken to process this trigger: {}s'.format(round(dxt_trigger_time, 5))
                    }
                )
            recommendation = [
                {
                    'message': 'Use collective write operations (e.g. MPI_File_write_all() or MPI_File_write_at_all()) and set one aggregator per compute node',
                    'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-collective-write.c'), line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
            )
    else:
        issue = 'Application uses MPI-IO and write data using {} ({:.2f}%) collective operations'.format(
            mpiio_coll_writes,
            mpiio_coll_writes / total_mpiio_write_operations * 100
        )

        insights_operation.append(
            message(INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE, TARGET_DEVELOPER, OK, issue)
        )


def check_mpi_none_block_operation(mpiio_nb_reads, mpiio_nb_writes, has_hdf5_extension, modules):
    '''
    Check whether application can benefit from non-blocking requests

    Parameters:
        mpiio_nb_reads: number of non-blocking mpi read operations
        mpiio_nb_writes: number of non-blocking mpi write operations
        has_hdf5_extension: boolean value of whether the file in in hdf5 extension
        modules: all different mudules been used in the application
    '''

    if mpiio_nb_reads == 0:
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
            message(INSIGHTS_MPI_IO_BLOCKING_READ_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
        )

    if mpiio_nb_writes == 0:
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
            message(INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE, TARGET_DEVELOPER, WARN, issue, recommendation)
        )


def check_mpi_aggregator(cb_nodes, NUMBER_OF_COMPUTE_NODES):
    '''
    Check whether application has used inter-node aggregators

    Parameters:
        cb_nodes: 
        NUMBER_OF_COMPUTE_NODES:
    '''

    if cb_nodes > NUMBER_OF_COMPUTE_NODES:
        issue = 'Application is using inter-node aggregators (which require network communication)'

        recommendation = [
            {
                'message': 'Set the MPI hints for the number of aggregators as one per compute node (e.g., cb_nodes={})'.format(
                    NUMBER_OF_COMPUTE_NODES
                ),
                'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-hints.bash'), line_numbers=True, background_color='default')
            }
        ]

        insights_operation.append(
            message(INSIGHTS_MPI_IO_AGGREGATORS_INTER, TARGET_USER, HIGH, issue, recommendation)
        )

    if cb_nodes < NUMBER_OF_COMPUTE_NODES:
        issue = 'Application is using intra-node aggregators'

        insights_operation.append(
            message(INSIGHTS_MPI_IO_AGGREGATORS_INTRA, TARGET_USER, OK, issue)
        )

    if cb_nodes == NUMBER_OF_COMPUTE_NODES:
        issue = 'Application is using one aggregator per compute node'

        insights_operation.append(
            message(INSIGHTS_MPI_IO_AGGREGATORS_OK, TARGET_USER, OK, issue)
        )


# Layout and export

def display_content(console):
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


def display_thresholds(console):
    tholdMessage = {
        'imbalance_operations': 'Minimum imbalance requests ratio:                [white]{}%[/white]'.format(thresholds['imbalance_operations'][0] * 100),
        'small_bytes': 'Minimum size of a small request:                 [white]{} bytes[/white]'.format(thresholds['small_bytes'][0]),
        'small_requests': 'Maximum small requests ratio:                    [white]{}%[/white]'.format(thresholds['small_requests'][0] * 100),
        'small_requests_absolute': 'Maximum small requests:                          [white]{}[/white]'.format(thresholds['small_requests_absolute'][0]),
        'misaligned_requests': 'Maximum misaligned requests ratio:               [white]{}%[/white]'.format(thresholds['misaligned_requests'][0] * 100),
        'random_operations': 'Maximum random request ratio:                    [white]{}%[/white]'.format(thresholds['random_operations'][0] * 100),
        'random_operations_absolute': 'Maximum random requests:                         [white]{}[/white]'.format(thresholds['random_operations_absolute'][0]),
        'metadata_time_rank': 'Maximum metadata process time per rank:          [white]{} seconds[/white]'.format(thresholds['metadata_time_rank'][0]),
        'imbalance_size': 'Maximum read/write size difference ratio:        [white]{}%[/white]'.format(thresholds['imbalance_size'][0] * 100),
        'imbalance_stragglers': 'Maximum ratio difference among ranks:            [white]{}%[/white]'.format(thresholds['imbalance_stragglers'][0] * 100),
        'interface_stdio': 'Maximum STDIO usage ratio:                       [white]{}%[/white]'.format(thresholds['interface_stdio'][0] * 100),
        'collective_operations': 'Minimum MPI collective operation usage ratio:    [white]{}%[/white]'.format(thresholds['collective_operations'][0] * 100),
        'collective_operations_absolute': 'Minimum MPI collective operations:               [white]{}[/white]'.format(thresholds['collective_operations_absolute'][0]),
    }

    toBeAppend = []
    if args.thold:
        for name, message in tholdMessage.items():
            toBeAppend.append(message)
    else:
        for name, message in tholdMessage.items():
            if thresholds[name][1]:
                toBeAppend.append(message)

    console.print(
        Panel(
            '\n'.join(toBeAppend),
            title='THRESHOLDS',
            title_align='left',
            padding=1
        )
    )


def display_footer(console, insights_start_time, insights_end_time):
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


def export_html(console, export_dir, trace_name):
    if not args.export_html:
        return

    os.makedirs(export_dir, exist_ok=True) # Ensure export directory exists
    filepath = os.path.join(export_dir, f"{trace_name}.html")

    console.save_html(
        filepath,
        theme=set_export_theme(),
        clear=False
    )


def export_svg(console, export_dir, trace_name):
    if not args.export_svg:
        return
    
    os.makedirs(export_dir, exist_ok=True) # Ensure export directory exists
    filepath = os.path.join(export_dir, f"{trace_name}.svg")

    console.save_svg(
        filepath,
        title='Drishti',
        theme=set_export_theme(),
        clear=False
    )


def export_csv(export_dir, trace_name, jobid=None):
    if not args.export_csv:
        return
    
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
    detected_issues['JOB'] = jobid

    for report in csv_report:
        detected_issues[report] = True

    
    os.makedirs(export_dir, exist_ok=True) # Ensure export directory exists
    filepath = os.path.join(export_dir, f"{trace_name}.csv")

    with open(filepath, 'w') as f:
        w = csv.writer(f)
        w.writerow(detected_issues.keys())
        w.writerow(detected_issues.values())


# =========================================================================
# Cost-based small-operation analysis (per-call trace paths only)
#
# The functions below supersede check_small_operation for handlers that
# have per-call durations, timestamps, thread ids, and library-layer
# intervals available (currently the eBPF/.pfw path). Darshan and
# Recorder handlers keep calling check_small_operation above; nothing in
# this section changes their behavior.
#
#  1. COST, NOT COUNT -- severity keys on the fraction of I/O *time*
#     spent in small requests. Numerous-but-cheap small ops are
#     explicitly reported as low priority.
#  2. LATENCY-MODE CLASSIFICATION -- small READS in the fast latency mode
#     were likely served by page cache/readahead and are excluded from
#     the costly set (they are free). Small WRITES in the fast mode were
#     absorbed by the page cache: NOT free but deferred -- their bill is
#     the fsync/fdatasync/close time on the same files, which is
#     attributed back to them proportionally to bytes written. Slow small
#     writes (throttling, sync flags, RMW fault-in) are charged directly.
#  3. ATTRIBUTION -- costly small ops are attributed to the top-level
#     library layer (HDF5 / MPI-IO / application) enclosing them on the
#     same thread, so recommendations target whoever issues the requests.
#  4. SYSTEM-DERIVED THRESHOLD -- "small" is derived from the request
#     size where measured throughput saturates in this trace; the
#     configured small_bytes is only a fallback.
# =========================================================================

import math

INSIGHTS_SMALL_READ_HIGH_COST = 'E01'
INSIGHTS_SMALL_WRITE_HIGH_COST = 'E02'
INSIGHTS_SMALL_OPERATION_LOW_IMPACT = 'E03'

thresholds['small_time_fraction'] = [0.2, False]      # fraction of direction time in costly small ops
thresholds['cache_latency_cutoff'] = [100e-6, False]  # fallback latency boundary for fast/slow mode split (seconds)
thresholds['knee_throughput_fraction'] = [0.5, False] # bucket counts as saturated at this fraction of peak throughput
thresholds['knee_min_bucket_ops'] = [5, False]        # min ops for a size bucket to participate in knee detection

SYNC_FUNCTIONS = ('fsync', 'fdatasync', 'close')


def derive_small_threshold(df_ops):
    """
    Derive the "small request" boundary from the trace itself: bucket
    requests by log2(size), compute the median throughput per bucket, and
    return the smallest bucket whose throughput reaches
    knee_throughput_fraction of the best bucket's. Everything below is
    per-request-overhead dominated on THIS system -- the empirical
    meaning of "small". Returns (threshold_bytes, derived: bool), falling
    back to the configured small_bytes when the trace lacks size
    diversity.
    """
    df = df_ops[(df_ops['size'] > 0) & (df_ops['duration'] > 0)]

    if not len(df):
        return thresholds['small_bytes'][0], False

    buckets = df.groupby(df['size'].map(lambda s: int(math.floor(math.log2(s)))))

    bucket_throughput = {}
    for bucket, group in buckets:
        if len(group) < thresholds['knee_min_bucket_ops'][0]:
            continue
        bucket_throughput[bucket] = (group['size'] / group['duration']).median()

    if len(bucket_throughput) < 3:
        return thresholds['small_bytes'][0], False

    peak = max(bucket_throughput.values())

    for bucket in sorted(bucket_throughput.keys()):
        if bucket_throughput[bucket] >= peak * thresholds['knee_throughput_fraction'][0]:
            return 2 ** bucket, True

    return thresholds['small_bytes'][0], False


def split_latency_modes(durations):
    """
    Split call latencies into a fast and a slow mode. If the distribution
    is clearly bimodal in log space (>= 8x jump between consecutive
    latencies), split at the largest gap; otherwise fall back to the
    absolute cache_latency_cutoff.

    For reads: fast mode ~= served from page cache / readahead (free).
    For writes: fast mode ~= absorbed by page cache (cost deferred to
    fsync/close/writeback); slow mode ~= blocked in the call (dirty-page
    throttling, sync flags, read-modify-write fault-in).

    Returns (mask_fast: pd.Series[bool], cutoff_used: float).

    NOTE: latency-based proxy until a page-cache tracepoint pass
    (mm_filemap_add_to_page_cache) provides per-call ground truth.
    """
    cutoff = thresholds['cache_latency_cutoff'][0]

    positive = durations[durations > 0]
    if len(positive) >= 10:
        logs = positive.map(math.log).sort_values().reset_index(drop=True)
        gaps = logs.diff()
        max_gap_pos = gaps.idxmax()
        if gaps.max() >= math.log(8) and 0 < max_gap_pos < len(logs):
            cutoff = math.exp((logs[max_gap_pos - 1] + logs[max_gap_pos]) / 2)

    return durations < cutoff, cutoff


def find_top_level_layers(df_layers):
    """
    Reduce layer events to their top-level (outermost) intervals per
    thread: an event is top-level if not contained in any earlier layer
    event on the same tid. Events on one tid are properly nested (call
    stack), so a linear sweep over start-sorted events suffices.
    """
    if df_layers is None or not len(df_layers):
        return pd.DataFrame(columns=['rank', 'tid', 'cat', 'function', 'start', 'end'])

    roots = []
    for tid, group in df_layers.groupby('tid'):
        group = group.sort_values('start')
        current_root_end = -1.0
        for row in group.itertuples(index=False):
            if row.start >= current_root_end:
                roots.append(row)
                current_root_end = row.end

    return pd.DataFrame(roots)


def attribute_to_layers(df_ops, df_layers):
    """
    Attribute each operation to the top-level library layer whose
    interval encloses its start on the same thread. Operations with no
    enclosing layer attribute to 'application'. Returns df_ops with an
    added 'layer' column.
    """
    df_ops = df_ops.copy()
    df_ops['layer'] = 'application'

    df_roots = find_top_level_layers(df_layers)
    if not len(df_roots):
        return df_ops

    attributed = []
    for tid, ops in df_ops.groupby('tid'):
        roots = df_roots[df_roots['tid'] == tid].sort_values('start')
        if not len(roots):
            attributed.append(ops)
            continue

        ops = ops.sort_values('start')
        merged = pd.merge_asof(
            ops,
            roots[['start', 'end', 'function']].rename(
                columns={'start': 'layer_start', 'end': 'layer_end', 'function': 'layer_function'}
            ),
            left_on='start',
            right_on='layer_start',
            direction='backward'
        )

        contained = merged['layer_end'] >= merged['start']
        merged.loc[contained, 'layer'] = merged.loc[contained, 'layer_function']
        attributed.append(merged[list(df_ops.columns)])

    return pd.concat(attributed, ignore_index=True)


def _small_cost_recommendations(layer_summary, is_read):
    recommendations = []

    top_layer = layer_summary.index[0] if len(layer_summary) else 'application'

    if top_layer.lower().startswith('h5'):
        recommendations.append({
            'message': 'Most costly small {} originate inside HDF5 ({}) -- application-side buffering will not help; '
                       'tune HDF5 instead (chunk cache via H5Pset_cache/H5Pset_chunk_cache, collective metadata, '
                       'or paged aggregation via H5Pset_file_space_strategy)'.format(
                           'reads' if is_read else 'writes', top_layer)
        })
    elif top_layer.lower().startswith('mpi'):
        recommendations.append({
            'message': 'Most costly small {} are issued by the MPI-IO layer ({}) -- check collective buffering and '
                       'data sieving hints (cb_buffer_size, ind_rd_buffer_size / ind_wr_buffer_size) rather than '
                       'application code'.format('reads' if is_read else 'writes', top_layer)
        })
    else:
        recommendations.append({
            'message': 'Costly small {} are issued directly by the application -- consider buffering/aggregating '
                       'them into larger requests, or delegating aggregation to a library (MPI-IO collectives, '
                       'HDF5)'.format('reads' if is_read else 'writes')
        })

    return recommendations


def _small_write_deferred_cost(df_small_fast, df_dir, df_posix_records, file_map):
    """
    Price the deferred cost of cache-absorbed small writes: fsync /
    fdatasync / close time on the files they wrote. Attribution is
    time-windowed: each sync call's duration is split by the byte shares
    of writes issued to that file since the previous sync -- a sync only
    flushes what was dirtied before it, so whole-run byte proportions
    would misattribute (e.g. large writes issued after the last fsync
    would dilute the small writes' bill). Returns (deferred_time_total,
    details list).
    """
    if df_posix_records is None or not len(df_small_fast):
        return 0.0, []

    df_sync = df_posix_records[df_posix_records['function'].str.contains('|'.join(SYNC_FUNCTIONS))]
    if not len(df_sync):
        return 0.0, []

    deferred_total = 0.0
    details = []

    fname_to_fid = {v: k for k, v in file_map.items()}

    for fname, syncs in df_sync.groupby('fname'):
        fid = fname_to_fid.get(fname)
        if fid is None:
            continue

        writes_all = df_dir[df_dir['file_id'] == fid].sort_values('start')
        writes_small = df_small_fast[df_small_fast['file_id'] == fid]
        if not len(writes_small):
            continue

        attributed_file = 0.0
        window_start = float('-inf')

        for sync in syncs.sort_values('start').itertuples(index=False):
            in_window_all = writes_all[(writes_all['start'] >= window_start) & (writes_all['start'] < sync.start)]
            in_window_small = writes_small[(writes_small['start'] >= window_start) & (writes_small['start'] < sync.start)]
            window_start = sync.start

            total_bytes = in_window_all['size'].sum()
            if not total_bytes:
                continue

            attributed_file += sync.duration * (in_window_small['size'].sum() / total_bytes)

        if attributed_file > 0:
            deferred_total += attributed_file
            details.append({'message': '{:.3f} s of fsync/close time on {} attributed to small writes '
                                       'dirtied before each sync'.format(attributed_file, fname)})

    return deferred_total, details


def _small_cost_report_direction(df_dir, df_layers, df_posix_records, file_map, is_read, small_threshold, derived):
    direction = 'read' if is_read else 'write'

    total_ops = len(df_dir)
    total_time = df_dir['duration'].sum()

    if not total_ops or total_time <= 0:
        return

    df_small = df_dir[df_dir['size'] < small_threshold]

    if not len(df_small):
        return

    fast_mask, cutoff_used = split_latency_modes(df_small['duration'])
    df_slow = df_small[~fast_mask]
    df_fast = df_small[fast_mask]

    small_time = df_small['duration'].sum()

    deferred_time = 0.0
    deferred_details = []
    if is_read:
        # Fast small reads were served from cache/readahead -- free.
        df_costly = df_slow
    else:
        # Fast small writes were absorbed by the page cache -- deferred,
        # not free. Their bill is the flush time on the same files.
        df_costly = df_slow
        deferred_time, deferred_details = _small_write_deferred_cost(
            df_fast, df_dir, df_posix_records, file_map
        )

    costly_time = df_costly['duration'].sum() + deferred_time
    time_fraction = costly_time / (total_time + deferred_time)

    threshold_note = 'small = < {} ({})'.format(
        convert_bytes(small_threshold),
        'derived from the throughput saturation point of this trace' if derived
        else 'configured default; trace had insufficient size diversity to derive one'
    )

    details = [
        {'message': threshold_note},
        {'message': '{} of {} {}s are small; in-call time {:.3f} s ({:.2f}% of {} time)'.format(
            len(df_small), total_ops, direction, small_time, small_time / total_time * 100, direction)},
    ]

    if is_read and len(df_fast):
        details.append({'message': '{} of the small reads sit in the fast latency mode (< {:.0f} us) and were '
                                   'likely served by page cache/readahead -- excluded from the costly set'.format(
                                       len(df_fast), cutoff_used * 1e6)})
    if not is_read:
        if len(df_slow):
            details.append({'message': '{} small writes blocked in the call itself (>= {:.0f} us: dirty-page '
                                       'throttling, sync flags, or read-modify-write)'.format(
                                           len(df_slow), cutoff_used * 1e6)})
        if len(df_fast):
            details.append({'message': '{} small writes were absorbed by the page cache; their deferred flush '
                                       'cost is {:.3f} s'.format(len(df_fast), deferred_time)})
        details.extend(deferred_details)

    if time_fraction < thresholds['small_time_fraction'][0]:
        if len(df_small) > thresholds['small_requests_absolute'][0]:
            issue = 'Application issues many small {}s ({}) but they cost only {:.2f}% of {} time ' \
                    '(deferred flush cost included) -- optimizing them is unlikely to improve performance'.format(
                        direction, len(df_small), time_fraction * 100, direction)

            insights_operation.append(
                message(INSIGHTS_SMALL_OPERATION_LOW_IMPACT, TARGET_DEVELOPER, OK, issue, None, details)
            )
        return

    thresholds['small_time_fraction'][1] = True

    df_attr = attribute_to_layers(df_costly if is_read else df_small, df_layers)
    layer_summary = df_attr.groupby('layer')['duration'].sum().sort_values(ascending=False)

    attr_time = layer_summary.sum()
    for layer, layer_time in layer_summary.head(3).items():
        details.append({'message': 'layer [b]{}[/b] accounts for {:.2f}% of the costly small-{} time'.format(
            layer, layer_time / attr_time * 100 if attr_time else 0, direction)})

    top_files = (df_costly if is_read else df_small).groupby('file_id')['duration'].sum().sort_values(ascending=False).head(3)
    for fid, ftime in top_files.items():
        if fid in file_map:
            details.append({'message': '{:.3f} s of costly small-{} time in {}'.format(ftime, direction, file_map[fid])})

    issue = 'Small {}s (< {}) consume {:.2f}% of {} time ({:.3f} s{})'.format(
        direction,
        convert_bytes(small_threshold),
        time_fraction * 100,
        direction,
        costly_time,
        ', including {:.3f} s of deferred flush cost'.format(deferred_time) if deferred_time else ''
    )

    recommendations = _small_cost_recommendations(layer_summary, is_read)

    if not is_read and deferred_time and deferred_time > df_costly['duration'].sum() - deferred_time:
        recommendations.append({
            'message': 'Most of the small-write cost is deferred flush time -- batch fsync/fdatasync calls or '
                       'reconsider per-record durability requirements before restructuring the writes themselves'
        })

    insights_operation.append(
        message(
            INSIGHTS_SMALL_READ_HIGH_COST if is_read else INSIGHTS_SMALL_WRITE_HIGH_COST,
            TARGET_DEVELOPER,
            HIGH,
            issue,
            recommendations,
            details
        )
    )


def check_small_operation_cost(df_posix, df_layers, df_posix_records, file_map):
    """
    Cost-based small-operation check for per-call trace paths (eBPF).

    Parameters:
        df_posix: POSIX data-transfer intervals (columns: function, size,
                  duration, start, tid, file_id)
        df_layers: library-layer intervals (may be empty/None)
        df_posix_records: all POSIX-category calls incl. fsync/close, with
                  resolved fname -- used to price deferred write cost
        file_map: dict of (id, path) pairs
    """
    df_reads = df_posix[(df_posix['function'].str.contains('read'))]
    df_writes = df_posix[~(df_posix['function'].str.contains('read'))]

    read_threshold, read_derived = derive_small_threshold(df_reads)

    # For writes, derive the knee from storage-bound (slow-mode) writes
    # only: cache-absorbed writes complete at memcpy speed regardless of
    # size, so including them distorts the throughput saturation curve.
    fast_mask_w, _ = split_latency_modes(df_writes['duration'])
    df_writes_slow = df_writes[~fast_mask_w]
    write_threshold, write_derived = derive_small_threshold(
        df_writes_slow if len(df_writes_slow) >= thresholds['knee_min_bucket_ops'][0] else df_writes
    )

    _small_cost_report_direction(df_reads, df_layers, df_posix_records, file_map, True, read_threshold, read_derived)
    _small_cost_report_direction(df_writes, df_layers, df_posix_records, file_map, False, write_threshold, write_derived)


# =========================================================================
# Collective vs independent MPI-IO: cost-based decision (per-call paths)
#
# The existing check_mpi_collective_read/write_operation above fire on a
# single condition -- zero collective calls plus enough independent ones
# -- and treat any collective usage as fine. That embeds the assumption
# that collectives are always the right answer, which is false: two-phase
# I/O trades a data shuffle plus a synchronization point for a better file
# access pattern, and that trade only pays off under specific conditions.
#
# The functions below gate the recommendation on four measured conditions:
#   1. IS THE INDEPENDENT PATTERN ACTUALLY BAD -- collectives create
#      contiguity by redistributing data. That only helps if the file
#      regions are interleaved across ranks or the requests are small. A
#      workload where every rank already reads a large contiguous block
#      has nothing to gain and pays the shuffle for free.
#   2. DOES THE FILESYSTEM REWARD IT -- two-phase assumes the backend is
#      much faster for few large contiguous streams and that many
#      interleaved clients contend on locks. True on Lustre/GPFS, false on
#      NFS, where ROMIO falls back to fcntl locking and close-to-open
#      consistency, so collective buffering often serializes instead.
#   3. CAN THE APPLICATION AFFORD THE SYNC -- a collective makes every
#      rank wait for the slowest. When per-rank I/O time is already
#      imbalanced (the same signal check_shared_time_imbalance reports),
#      that wait is charged on every collective call.
#   4. IS THERE ENOUGH DATA -- shuffle and synchronization are largely
#      fixed costs per call, so tiny collectives cannot amortize them.
#
# The benefit estimate is deliberately conservative: aggregated I/O time
# is modeled as if a SINGLE aggregator moved all the bytes at the peak
# per-stream throughput measured elsewhere in the same trace. Real
# collective I/O uses several aggregators and is faster than that, so when
# this pessimistic model still beats the observed independent time, the
# case for collectives is solid rather than speculative.
# =========================================================================

INSIGHTS_MPI_IO_COLLECTIVE_READ_RECOMMENDED = 'E04'
INSIGHTS_MPI_IO_COLLECTIVE_READ_NOT_BENEFICIAL = 'E05'
INSIGHTS_MPI_IO_COLLECTIVE_WRITE_RECOMMENDED = 'E06'
INSIGHTS_MPI_IO_COLLECTIVE_WRITE_NOT_BENEFICIAL = 'E07'

thresholds['collective_interleave'] = [0.3, False]        # fraction of offset-adjacent request pairs owned by different ranks
thresholds['collective_min_bytes'] = [16777216, False]     # minimum bytes on a file before collectives can amortize their fixed cost
thresholds['collective_small_fraction'] = [0.5, False]     # fraction of small requests that also makes aggregation worthwhile
thresholds['collective_min_gain'] = [0.1, False]           # estimated gain must be this fraction of the phase before recommending a code change
thresholds['collective_sync_dominance'] = [0.5, False]     # arrival skew above this fraction of the phase means compute imbalance, not I/O, is the problem

# Collective calls are the ones ROMIO synchronizes across the communicator.
COLLECTIVE_MARKERS = ('_all', '_ordered')

# What each filesystem class does to the two-phase trade. 'favors' is the
# lock/metadata policy verdict for gate 2; None means unknown and the check
# reports its verdict as conditional instead of firm.
FILESYSTEM_COLLECTIVE_POLICY = {
    'lustre': (True,
               'Lustre serializes overlapping extent locks per OST, so many ranks reading interleaved '
               'regions contend; two-phase gives each aggregator a contiguous file domain and removes that contention'),
    'gpfs': (True,
             'GPFS byte-range tokens are revoked and reacquired when ranks interleave within a block; '
             'aggregating into contiguous file domains keeps tokens stable'),
    'beegfs': (True,
               'Chunk-based striping rewards few large contiguous streams over many interleaved clients'),
    'nfs': (False,
            'NFS has no distributed lock manager comparable to a parallel filesystem: ROMIO falls back to '
            'fcntl locking and close-to-open consistency forces revalidation, so collective buffering '
            'commonly serializes rather than aggregates'),
    'local': (False,
              'A local filesystem has no cross-node lock or striping behaviour for two-phase to exploit'),
}


def is_collective_call(function_name):
    return any(marker in function_name for marker in COLLECTIVE_MARKERS)


def measure_reference_throughput(df_ops):
    """
    Peak per-operation throughput observed in this trace, taken as the
    best median across log2 size buckets. Used as the "what this system
    delivers for well-formed requests" reference that aggregated I/O
    would approach.
    """
    df = df_ops[(df_ops['size'] > 0) & (df_ops['duration'] > 0)]
    if not len(df):
        return None

    best = None
    for bucket, group in df.groupby(df['size'].map(lambda s: int(math.floor(math.log2(s))))):
        if len(group) < thresholds['knee_min_bucket_ops'][0]:
            continue
        bucket_bw = (group['size'] / group['duration']).median()
        if best is None or bucket_bw > best:
            best = bucket_bw

    return best


def interleave_ratio(df_file):
    """
    Fraction of offset-adjacent request pairs that belong to different
    ranks. High values mean the file's regions are fragmented across
    ranks -- the pattern two-phase aggregation exists to fix. Low values
    mean each rank already owns a contiguous span, so a shuffle would
    only move data around for no structural gain.
    """
    df = df_file.sort_values('offset')
    ranks = df['rank'].values
    if len(ranks) < 2:
        return 0.0
    return float((ranks[1:] != ranks[:-1]).mean())


def _analyze_collective_candidate(df_file, reference_bw, small_threshold):
    """
    Measure the four gates for one file's independent operations.
    Returns a facts dict, or None when the file cannot be a candidate.
    """
    n_ranks = df_file['rank'].nunique()
    if n_ranks < 2:
        return None

    total_bytes = int(df_file['size'].sum())
    if total_bytes <= 0:
        return None

    per_rank = df_file.groupby('rank').agg(
        io_time=('duration', 'sum'),
        first_start=('start', 'min'),
        last_end=('end', 'max'),
    )

    # Elapsed time of this phase: ranks read concurrently, so the phase
    # spans from the first rank entering it to the last rank leaving.
    observed_wall = float(per_rank['last_end'].max() - per_rank['first_start'].min())
    if observed_wall <= 0:
        return None

    # Gate 3 -- separate ARRIVAL skew from SERVICE skew. A collective
    # completes when its slowest participant does, but only arrival skew
    # survives the change: time a rank spends between its I/O calls is
    # compute, which aggregation does not touch, so a rank that shows up
    # late still makes everyone wait. Service skew (a rank whose I/O is
    # simply slow) is exactly what aggregation is meant to remove, so
    # charging it as synchronization cost would double-count it.
    per_rank['non_io'] = (per_rank['last_end'] - per_rank['first_start']) - per_rank['io_time']
    arrival_skew = max(0.0, float(per_rank['non_io'].max() - per_rank['non_io'].median()))
    service_skew = max(0.0, float(per_rank['io_time'].max() - per_rank['io_time'].median()))
    sync_cost = arrival_skew

    facts = {
        'n_ranks': n_ranks,
        'n_ops': len(df_file),
        'total_bytes': total_bytes,
        'observed_wall': observed_wall,
        'sync_cost': sync_cost,
        'arrival_skew': arrival_skew,
        'service_skew': service_skew,
        'interleave': interleave_ratio(df_file),
        'small_fraction': float((df_file['size'] < small_threshold).mean()),
        'observed_bw': total_bytes / observed_wall if observed_wall > 0 else 0.0,
        'reference_bw': reference_bw,
    }

    # Conservative model: one aggregator moving every byte at the best
    # per-stream throughput this trace demonstrates. Real runs use several
    # aggregators, so this overstates the collective cost on purpose.
    if reference_bw:
        facts['aggregated_io_time'] = total_bytes / reference_bw
        facts['estimated_collective_time'] = facts['aggregated_io_time'] + sync_cost
        facts['estimated_gain'] = observed_wall - facts['estimated_collective_time']
    else:
        facts['aggregated_io_time'] = None
        facts['estimated_collective_time'] = None
        facts['estimated_gain'] = None

    # Gate 1: collectives restructure the access pattern, so they need a
    # pattern worth restructuring -- fragmented across ranks, or small.
    facts['pattern_is_bad'] = (
        facts['interleave'] >= thresholds['collective_interleave'][0]
        or facts['small_fraction'] >= thresholds['collective_small_fraction'][0]
    )

    # Gate 4: fixed per-call costs need enough data to amortize.
    facts['enough_data'] = total_bytes >= thresholds['collective_min_bytes'][0]

    return facts


def _collective_facts_details(facts, small_threshold, fs_type, fs_note):
    details = [
        {'message': '{} independent requests from {} ranks moving {}'.format(
            facts['n_ops'], facts['n_ranks'], convert_bytes(facts['total_bytes']))},
        {'message': '{:.0f}% of offset-adjacent requests belong to different ranks (interleaved access '
                    'is what two-phase aggregation fixes)'.format(facts['interleave'] * 100)},
        {'message': '{:.0f}% of the requests are below {}'.format(
            facts['small_fraction'] * 100, convert_bytes(small_threshold))},
    ]

    if facts['reference_bw']:
        details.append({'message': 'observed {}/s aggregate vs {}/s peak per-stream throughput measured '
                                   'elsewhere in this trace'.format(
                                       convert_bytes(int(facts['observed_bw'])),
                                       convert_bytes(int(facts['reference_bw'])))})

    details.append({'message': 'rank skew: {:.3f} s arrival (compute imbalance -- a collective charges this as '
                               'synchronization wait) vs {:.3f} s service (slow I/O -- aggregation is meant to '
                               'remove this)'.format(facts['arrival_skew'], facts['service_skew'])})

    if fs_type:
        details.append({'message': 'filesystem [b]{}[/b]: {}'.format(fs_type, fs_note)})
    else:
        details.append({'message': 'filesystem unknown -- pass --fs-type for a firm verdict, since lock and '
                                   'metadata policy decides whether aggregation pays off'})

    return details


def _report_collective_direction(df_mpiio_dir, df_all_ops, file_map, direction, fs_type):
    """
    Evaluate the collective-vs-independent decision for one direction and
    emit either a recommendation to adopt collectives or an explicit
    finding that they would not help.
    """
    is_read = direction == 'read'

    df_indep = df_mpiio_dir[~df_mpiio_dir['function'].map(is_collective_call)]
    df_coll = df_mpiio_dir[df_mpiio_dir['function'].map(is_collective_call)]

    if not len(df_indep):
        return

    reference_bw = measure_reference_throughput(df_all_ops)
    small_threshold, _ = derive_small_threshold(df_mpiio_dir)

    fs_favors, fs_note = FILESYSTEM_COLLECTIVE_POLICY.get(fs_type, (None, None))

    for fid, df_file in df_indep.groupby('file_id'):
        facts = _analyze_collective_candidate(df_file, reference_bw, small_threshold)
        if facts is None:
            continue

        fname = file_map.get(fid, str(fid))
        already_collective = len(df_coll[df_coll['file_id'] == fid]) > 0

        details = _collective_facts_details(facts, small_threshold, fs_type, fs_note)
        if already_collective:
            details.append({'message': 'this file also sees {} collective calls -- the independent ones are a '
                                       'mixed-mode phase'.format(len(df_coll[df_coll['file_id'] == fid]))})

        # ---- Gate evaluation, in order of how decisively each one settles it ----

        # Gate 2 first: on a filesystem that punishes collective buffering,
        # the pattern analysis does not matter.
        if fs_favors is False:
            issue = 'Independent {}s on {} are NOT worth converting to collectives on this filesystem'.format(
                direction, fname)
            recommendation = [
                {'message': 'Keep independent {}s here, or explicitly disable collective buffering '
                            '(romio_cb_{} = disable) if a library enables it by default'.format(
                                direction, 'read' if is_read else 'write')},
            ]
            insights_operation.append(
                message(
                    INSIGHTS_MPI_IO_COLLECTIVE_READ_NOT_BENEFICIAL if is_read
                    else INSIGHTS_MPI_IO_COLLECTIVE_WRITE_NOT_BENEFICIAL,
                    TARGET_DEVELOPER, INFO, issue, recommendation, details)
            )
            continue

        # Gate 1: nothing to restructure.
        if not facts['pattern_is_bad']:
            issue = 'Independent {}s on {} already use a rank-contiguous, large-request pattern -- ' \
                    'collectives would add a shuffle without improving the file access'.format(direction, fname)
            insights_operation.append(
                message(
                    INSIGHTS_MPI_IO_COLLECTIVE_READ_NOT_BENEFICIAL if is_read
                    else INSIGHTS_MPI_IO_COLLECTIVE_WRITE_NOT_BENEFICIAL,
                    TARGET_DEVELOPER, OK, issue, None, details)
            )
            continue

        # Gate 4: too little data to amortize the fixed cost.
        if not facts['enough_data']:
            issue = 'Independent {}s on {} show an aggregatable pattern but only move {} -- too little to ' \
                    'amortize the shuffle and synchronization cost'.format(
                        direction, fname, convert_bytes(facts['total_bytes']))
            insights_operation.append(
                message(
                    INSIGHTS_MPI_IO_COLLECTIVE_READ_NOT_BENEFICIAL if is_read
                    else INSIGHTS_MPI_IO_COLLECTIVE_WRITE_NOT_BENEFICIAL,
                    TARGET_DEVELOPER, OK, issue, None, details)
            )
            continue

        # Gate 3: the synchronization the collective imposes costs more
        # than the better access pattern would recover. Two ways that
        # happens: the arrival skew dominates the phase outright, or the
        # modeled gain survives but is too small to justify a code change
        # given the model's uncertainty.
        sync_dominates = (
            facts['arrival_skew'] / facts['observed_wall'] >= thresholds['collective_sync_dominance'][0]
        )
        marginal_gain = (
            facts['estimated_gain'] is not None
            and 0 < facts['estimated_gain'] / facts['observed_wall'] < thresholds['collective_min_gain'][0]
        )

        if sync_dominates or marginal_gain or (facts['estimated_gain'] is not None and facts['estimated_gain'] <= 0):
            if sync_dominates:
                issue = 'Independent {}s on {} show an aggregatable pattern, but {:.3f} s of the {:.3f} s phase ' \
                        'is ranks arriving at different times -- a collective would spend that as synchronization ' \
                        'wait, and only {:.3f} s of actual I/O is left for aggregation to improve'.format(
                            direction, fname, facts['arrival_skew'], facts['observed_wall'],
                            facts['aggregated_io_time'] if facts['aggregated_io_time'] else 0.0)
                recommendation = [
                    {'message': 'Balance the compute phase feeding this {} first -- the imbalance is in when ranks '
                                'reach the call, not in how fast their I/O runs, so aggregation cannot recover '
                                'it'.format(direction)}
                ]
            else:
                issue = 'Independent {}s on {} show an aggregatable pattern, but the estimated benefit ({:.3f} s of ' \
                        'a {:.3f} s phase) is too small to justify restructuring the calls'.format(
                            direction, fname, max(0.0, facts['estimated_gain']), facts['observed_wall'])
                recommendation = [
                    {'message': 'Leave these {}s independent unless the phase grows; re-evaluate if the data volume '
                                'or rank count increases'.format(direction)}
                ]
            insights_operation.append(
                message(
                    INSIGHTS_MPI_IO_COLLECTIVE_READ_NOT_BENEFICIAL if is_read
                    else INSIGHTS_MPI_IO_COLLECTIVE_WRITE_NOT_BENEFICIAL,
                    TARGET_DEVELOPER, WARN, issue, recommendation, details)
            )
            continue

        # All gates passed.
        thresholds['collective_interleave'][1] = True
        thresholds['collective_min_bytes'][1] = True

        if facts['estimated_gain'] is not None:
            issue = 'Independent {}s on {} should be collective: even assuming a single aggregator at the ' \
                    'peak per-stream throughput of this trace, aggregation would cut {:.3f} s from the ' \
                    'observed {:.3f} s (synchronization cost already deducted)'.format(
                        direction, fname, facts['estimated_gain'], facts['observed_wall'])
        else:
            issue = 'Independent {}s on {} use an interleaved pattern across {} ranks that collective I/O ' \
                    'is designed to aggregate'.format(direction, fname, facts['n_ranks'])

        recommendation = [
            {'message': 'Replace MPI_File_{}* with the collective MPI_File_{}_all variant on this file'.format(
                'read' if is_read else 'write', 'read' if is_read else 'write'),
             'sample': Syntax.from_path(
                 os.path.join(ROOT, 'snippets/mpi-io-collective-read.c' if is_read else 'snippets/mpi-io-collective-write.c'),
                 line_numbers=True, background_color='default')},
        ]

        if fs_favors is None:
            recommendation.append({
                'message': 'Confirm with --fs-type: this verdict assumes the backend rewards few large '
                           'contiguous streams, which is true on Lustre/GPFS but not on NFS'
            })

        insights_operation.append(
            message(
                INSIGHTS_MPI_IO_COLLECTIVE_READ_RECOMMENDED if is_read
                else INSIGHTS_MPI_IO_COLLECTIVE_WRITE_RECOMMENDED,
                TARGET_DEVELOPER, HIGH, issue, recommendation, details)
        )


def check_mpi_collective_operation_cost(df_mpiio, df_all_ops, file_map, fs_type=None):
    """
    Cost-based replacement for check_mpi_collective_read_operation and
    check_mpi_collective_write_operation on per-call trace paths.

    Parameters:
        df_mpiio: MPI-IO intervals (columns: file_id, rank, function,
                  start, duration, size, offset)
        df_all_ops: every data-transfer interval in the trace, used to
                  measure the peak per-stream throughput this system
                  delivers for well-formed requests
        file_map: dict of (id, path) pairs
        fs_type: one of FILESYSTEM_COLLECTIVE_POLICY, or None when unknown
    """
    df_reads = df_mpiio[(df_mpiio['function'].str.contains('read'))]
    df_writes = df_mpiio[~(df_mpiio['function'].str.contains('read'))]

    _report_collective_direction(df_reads, df_all_ops, file_map, 'read', fs_type)
    _report_collective_direction(df_writes, df_all_ops, file_map, 'write', fs_type)


# =========================================================================
# Checks that only a per-call timeline makes possible
#
# None of these have a counter-based equivalent: they need per-call
# timestamps, open/close pairing, or the nesting of syscalls inside
# library-layer intervals. Darshan and Recorder cannot feed them, so they
# are only wired into the eBPF handler.
#
#   E08  I/O-compute serialization  -- storage idle while compute runs
#   E09  file reopen churn          -- the same file reopened in a loop
#   E10  sync overkill              -- durability paid per record
#   E11  software-stack overhead    -- time in library code, not in I/O
# =========================================================================

INSIGHTS_IO_COMPUTE_SERIALIZATION = 'E08'
INSIGHTS_IO_NO_OVERLAP_HEADROOM = 'E08b'
INSIGHTS_FILE_REOPEN_CHURN = 'E09'
INSIGHTS_SYNC_OVERKILL = 'E10'
INSIGHTS_STACK_OVERHEAD = 'E11'

thresholds['serialization_idle_fraction'] = [0.5, False]   # storage idle at least this fraction of the run
thresholds['serialization_min_io'] = [0.1, False]          # I/O must cost at least this fraction of the run to be worth hiding
thresholds['serialization_burst_gap'] = [0.01, False]      # gap (s) that separates one I/O burst from the next
thresholds['reopen_count'] = [10, False]                   # opens of the same file by the same rank before it counts as churn
thresholds['sync_min_bytes'] = [1048576, False]            # bytes per sync below this is durability paid too often
thresholds['sync_time_fraction'] = [0.1, False]            # sync time above this fraction of write time is worth reporting
thresholds['stack_overhead_fraction'] = [0.5, False]       # fraction of library time not spent in syscalls

EXACT_OPEN_FUNCTIONS = ('open', 'openat', 'openat2', 'open64', 'creat')
EXACT_SYNC_FUNCTIONS = ('fsync', 'fdatasync', 'sync', 'syncfs', 'msync')


def merge_intervals(df, gap=0.0):
    """
    Merge overlapping (or nearly overlapping) [start, end) intervals into
    disjoint spans with a single sweep. Returns (total_length, spans).
    Intervals closer together than `gap` are treated as one span, which is
    how I/O bursts are separated from the compute between them.
    """
    if df is None or not len(df):
        return 0.0, []

    ordered = df[['start', 'end']].sort_values('start').values

    spans = []
    current_start, current_end = float(ordered[0][0]), float(ordered[0][1])

    for start, end in ordered[1:]:
        if start <= current_end + gap:
            current_end = max(current_end, float(end))
        else:
            spans.append((current_start, current_end))
            current_start, current_end = float(start), float(end)

    spans.append((current_start, current_end))

    return sum(e - s for s, e in spans), spans


def check_io_compute_serialization(df_posix_records):
    """
    E08 -- Is the storage system idle while the application computes?

    Union every rank's I/O intervals to get the wall-clock time during
    which ANY I/O was in flight. The rest of the run is time the storage
    system spent doing nothing while it could have been prefetching or
    draining writes. With perfect overlap the run would take
    max(compute, io) instead of compute + io, so the recoverable time is
    min(compute, io) -- reported as the upper bound it is.
    """
    if df_posix_records is None or not len(df_posix_records):
        return

    run_span = float(df_posix_records['end'].max() - df_posix_records['start'].min())
    if run_span <= 0:
        return

    io_busy, spans = merge_intervals(df_posix_records)
    if io_busy <= 0:
        return

    _, bursts = merge_intervals(df_posix_records, gap=thresholds['serialization_burst_gap'][0])

    compute = max(0.0, run_span - io_busy)
    io_fraction = io_busy / run_span
    idle_fraction = compute / run_span
    recoverable = min(compute, io_busy)

    details = [
        {'message': 'traced span {:.3f} s: I/O in flight for {:.3f} s ({:.1f}%), no I/O in flight for '
                    '{:.3f} s ({:.1f}%)'.format(run_span, io_busy, io_fraction * 100, compute, idle_fraction * 100)},
        {'message': 'I/O arrives in {} burst(s) separated by gaps longer than {:.0f} ms'.format(
            len(bursts), thresholds['serialization_burst_gap'][0] * 1000)},
    ]

    if io_fraction < thresholds['serialization_min_io'][0]:
        insights_operation.append(
            message(INSIGHTS_IO_NO_OVERLAP_HEADROOM, TARGET_DEVELOPER, OK,
                    'I/O occupies only {:.1f}% of the traced span -- overlapping it with compute would recover '
                    'little'.format(io_fraction * 100),
                    None, details)
        )
        return

    if idle_fraction < thresholds['serialization_idle_fraction'][0]:
        insights_operation.append(
            message(INSIGHTS_IO_NO_OVERLAP_HEADROOM, TARGET_DEVELOPER, OK,
                    'I/O is already spread across {:.1f}% of the traced span -- there is little idle storage time '
                    'left to hide it in'.format(io_fraction * 100),
                    None, details)
        )
        return

    thresholds['serialization_idle_fraction'][1] = True

    issue = 'I/O and compute are serialized: the storage system is idle {:.1f}% of the run while {:.3f} s is ' \
            'spent waiting on I/O -- overlapping them could recover up to {:.3f} s'.format(
                idle_fraction * 100, io_busy, recoverable)

    recommendations = [
        {'message': 'Issue reads ahead of the compute that consumes them and drain writes behind it, so transfers '
                    'run during the {:.3f} s the storage system currently spends idle'.format(compute)},
        {'message': 'With MPI-IO, use the non-blocking or split-collective calls and place the wait after the '
                    'compute that does not depend on the data',
         'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-iread.c'),
                                    line_numbers=True, background_color='default')},
    ]

    insights_operation.append(
        message(INSIGHTS_IO_COMPUTE_SERIALIZATION, TARGET_DEVELOPER, HIGH, issue, recommendations, details)
    )


def check_file_reopen_churn(df_posix_records, file_map=None):
    """
    E09 -- The same file opened over and over by the same rank.

    A file opened once per iteration pays a full metadata round trip every
    time for nothing: the fd could have been held open across the loop.
    Falls straight out of the open/close records the parser already builds.
    """
    if df_posix_records is None or not len(df_posix_records):
        return

    df_open = df_posix_records[df_posix_records['function'].isin(EXACT_OPEN_FUNCTIONS)]
    if not len(df_open):
        return

    grouped = df_open.groupby(['rank', 'fname']).agg(
        opens=('function', 'size'),
        open_time=('duration', 'sum'),
    ).reset_index()

    churn = grouped[grouped['opens'] > thresholds['reopen_count'][0]]
    if not len(churn):
        return

    thresholds['reopen_count'][1] = True

    # Every open after the first is avoidable, so price the saving with
    # this file's own measured per-open latency.
    churn = churn.copy()
    churn['avoidable_time'] = churn['open_time'] * (churn['opens'] - 1) / churn['opens']

    per_file = churn.groupby('fname').agg(
        total_opens=('opens', 'sum'),
        ranks=('rank', 'nunique'),
        avoidable=('avoidable_time', 'sum'),
        max_opens_one_rank=('opens', 'max'),
    ).sort_values('avoidable', ascending=False)

    total_avoidable = float(per_file['avoidable'].sum())

    details = []
    for fname, row in per_file.head(5).iterrows():
        details.append({'message': '{}: {} opens across {} rank(s), up to {} by a single rank -- {:.3f} s '
                                   'avoidable'.format(fname, int(row['total_opens']), int(row['ranks']),
                                                      int(row['max_opens_one_rank']), row['avoidable'])})

    issue = '{} file(s) are reopened repeatedly by the same rank, spending {:.3f} s on opens that could be ' \
            'avoided by holding the descriptor'.format(len(per_file), total_avoidable)

    recommendations = [
        {'message': 'Open each file once outside the loop and keep the descriptor (or the HDF5/MPI file handle) '
                    'open across iterations; every open after the first is a metadata round trip for a file the '
                    'application already had'},
    ]

    insights_operation.append(
        message(INSIGHTS_FILE_REOPEN_CHURN, TARGET_DEVELOPER, WARN, issue, recommendations, details)
    )


def check_sync_overkill(df_posix_records, df_intervals=None, file_map=None):
    """
    E10 -- Durability requested more often than the data warrants.

    An fsync after every record forces a full write-out (and, on NFS, a
    COMMIT round trip) for a handful of bytes. Reports bytes-per-sync and
    what fraction of write time the syncs themselves consume.
    """
    if df_posix_records is None or not len(df_posix_records):
        return

    df_sync = df_posix_records[df_posix_records['function'].isin(EXACT_SYNC_FUNCTIONS)]
    if not len(df_sync):
        return

    sync_count = len(df_sync)
    sync_time = float(df_sync['duration'].sum())

    # Bytes written, per file, so bytes-per-sync is meaningful
    written_by_fname = {}
    write_time = 0.0
    if df_intervals is not None and len(df_intervals) and file_map:
        df_writes = df_intervals[~df_intervals['function'].str.contains('read')]
        write_time = float(df_writes['duration'].sum())
        for fid, size in df_writes.groupby('file_id')['size'].sum().items():
            if fid in file_map:
                written_by_fname[file_map[fid]] = int(size)

    total_written = sum(written_by_fname.values())
    bytes_per_sync = total_written / sync_count if sync_count else 0
    sync_share = sync_time / (write_time + sync_time) if (write_time + sync_time) > 0 else 0.0

    details = [
        {'message': '{} sync call(s) costing {:.3f} s'.format(sync_count, sync_time)},
    ]
    if total_written:
        details.append({'message': '{} written across the run -- {} per sync'.format(
            convert_bytes(total_written), convert_bytes(int(bytes_per_sync)))})
    if write_time > 0:
        details.append({'message': 'syncs account for {:.1f}% of write-plus-sync time'.format(sync_share * 100)})

    per_file = df_sync.groupby('fname').agg(syncs=('function', 'size'), time=('duration', 'sum'))
    for fname, row in per_file.sort_values('time', ascending=False).head(3).iterrows():
        written = written_by_fname.get(fname)
        suffix = ' for {}'.format(convert_bytes(written)) if written else ''
        details.append({'message': '{}: {} syncs costing {:.3f} s{}'.format(
            fname, int(row['syncs']), row['time'], suffix)})

    too_frequent = bool(total_written) and bytes_per_sync < thresholds['sync_min_bytes'][0]
    too_expensive = sync_share >= thresholds['sync_time_fraction'][0]

    if not (too_frequent or too_expensive):
        return

    thresholds['sync_min_bytes'][1] = True

    if too_frequent:
        issue = 'Durability is requested far more often than the data warrants: {} sync call(s) for {} of ' \
                'writes ({} per sync), costing {:.3f} s'.format(
                    sync_count, convert_bytes(total_written), convert_bytes(int(bytes_per_sync)), sync_time)
    else:
        issue = 'Sync calls consume {:.1f}% of write-plus-sync time ({:.3f} s across {} calls)'.format(
            sync_share * 100, sync_time, sync_count)

    recommendations = [
        {'message': 'Batch the syncs: flush once per checkpoint or per iteration rather than per record, so each '
                    'sync commits a useful amount of data'},
        {'message': 'Confirm the durability is needed at all -- if the file is rewritten or discarded on restart, '
                    'the data can be left to the page cache and flushed once at close'},
    ]

    insights_operation.append(
        message(INSIGHTS_SYNC_OVERKILL, TARGET_DEVELOPER, WARN, issue, recommendations, details)
    )


def compute_self_time(df_layers, df_posix_records):
    """
    Exclusive ("self") time per instrumented function: its own duration
    minus the time spent inside the calls nested within it.

    This is the only honest way to report where time goes when the
    instrumentation covers a library's INTERNAL functions and not just its
    API boundary. Inclusive time double-counts a caller and its callees,
    and "time in library code rather than syscalls" is trivially ~100% of
    inclusive time once internal helpers are traced.

    Returns (self_by_function, syscall_time_in_layers, layer_inclusive,
    thread_count).
    """
    frames = []
    if df_layers is not None and len(df_layers):
        layers = df_layers[['tid', 'function', 'start', 'end', 'duration']].copy()
        layers['is_syscall'] = False
        frames.append(layers)
    if df_posix_records is not None and len(df_posix_records) and 'tid' in df_posix_records.columns:
        calls = df_posix_records[['tid', 'function', 'start', 'end', 'duration']].copy()
        calls['is_syscall'] = True
        frames.append(calls)

    if not frames:
        return {}, 0.0, 0.0, 0

    events = pd.concat(frames, ignore_index=True)

    self_by_function = {}
    syscall_in_layers = 0.0
    layer_inclusive = 0.0
    threads = 0

    for tid, group in events.groupby('tid'):
        threads += 1

        # Parents before children: earlier start first, and on a tie the
        # longer interval (the enclosing one) first.
        group = group.sort_values(['start', 'end'], ascending=[True, False])

        stack = []          # [end, child_time, function, is_syscall]
        for row in group.itertuples(index=False):
            while stack and stack[-1][0] <= row.start:
                frame = stack.pop()
                self_time = max(0.0, frame[1])
                if not frame[3]:
                    self_by_function[frame[2]] = self_by_function.get(frame[2], 0.0) + self_time

            if stack:
                stack[-1][1] -= row.duration
                if row.is_syscall:
                    syscall_in_layers += row.duration
            elif not row.is_syscall:
                # A root layer interval: disjoint from the others on this
                # thread, so summing roots gives time inside the library
                # without double counting.
                layer_inclusive += row.duration

            stack.append([row.end, row.duration, row.function, row.is_syscall])

        while stack:
            frame = stack.pop()
            if not frame[3]:
                self_by_function[frame[2]] = self_by_function.get(frame[2], 0.0) + max(0.0, frame[1])

    return self_by_function, syscall_in_layers, layer_inclusive, threads


def check_software_stack_overhead(df_layers, df_posix_records):
    """
    E11 -- Where does the time inside the I/O software stack actually go?

    Library-layer events enclose the syscalls they trigger, so subtracting
    the syscall time from the library time shows how much of the run is
    spent in library machinery -- metadata traversal, datatype conversion,
    chunk cache and free-list management -- rather than moving data. No
    counter-based tool can separate the two.

    Two things this reports carefully, because getting them wrong makes
    the numbers meaningless:

      - Times are CUMULATIVE across threads. A 3 s run on 8 threads has
        24 thread-seconds available, so a cumulative figure larger than
        the wall-clock span is expected and is labelled as such.
      - Per-function figures are EXCLUSIVE (self) time. Datacrumbs traces
        internal library functions, not only the API boundary, so
        inclusive time would count a caller and everything it calls.
    """
    if df_layers is None or not len(df_layers):
        return
    if df_posix_records is None or not len(df_posix_records):
        return
    if 'tid' not in df_posix_records.columns:
        return

    self_by_function, syscall_in_layers, layer_inclusive, threads = compute_self_time(
        df_layers, df_posix_records)

    if layer_inclusive <= 0 or not self_by_function:
        return

    library_self = sum(self_by_function.values())
    overhead_fraction = library_self / layer_inclusive if layer_inclusive > 0 else 0.0

    # Wall-clock context, so a cumulative number can never be mistaken for
    # elapsed time.
    span_start = min(df_layers['start'].min(), df_posix_records['start'].min())
    span_end = max(df_layers['end'].max(), df_posix_records['end'].max())
    wall_span = float(span_end - span_start)

    ranked = sorted(self_by_function.items(), key=lambda kv: kv[1], reverse=True)

    details = [
        {'message': 'cumulative across {} thread(s) in a {:.3f} s run: {:.3f} thread-seconds inside instrumented '
                    'library calls, of which {:.3f} s is syscalls and {:.3f} s is library code '
                    '({:.1f}%)'.format(threads, wall_span, layer_inclusive, syscall_in_layers,
                                       library_self, overhead_fraction * 100)},
        {'message': 'that is {:.3f} s of library code per thread on average, against a {:.3f} s wall-clock '
                    'span'.format(library_self / threads if threads else 0.0, wall_span)},
        {'message': '{} distinct instrumented function(s) appear in the trace -- where internal helpers are traced '
                    'as well as the API boundary, most library time is expected to sit above the syscall '
                    'layer'.format(len(self_by_function))},
        {'message': 'exclusive (self) time by function, so a caller is not charged for what it calls:'},
    ]

    for name, self_time in ranked[:8]:
        details.append({'message': '{}: {:.3f} s self ({:.1f}% of library code)'.format(
            name, self_time, self_time / library_self * 100 if library_self else 0.0)})

    if overhead_fraction < thresholds['stack_overhead_fraction'][0]:
        insights_operation.append(
            message(INSIGHTS_STACK_OVERHEAD, TARGET_DEVELOPER, OK,
                    'The I/O stack passes work through to syscalls: {:.1f}% of the time inside instrumented '
                    'library calls is spent in the syscalls beneath them'.format((1 - overhead_fraction) * 100),
                    None, details)
        )
        return

    thresholds['stack_overhead_fraction'][1] = True

    top_name, top_self = ranked[0]
    top_share = top_self / library_self * 100 if library_self else 0.0

    recommendations = []
    if top_name.lower().startswith('h5'):
        recommendations.append({
            'message': 'The largest single consumer of library time is {} ({:.1f}% of it, {:.3f} s cumulative) -- '
                       'look there first rather than at the storage configuration'.format(
                           top_name, top_share, top_self)})
        if any(n.lower().startswith(('h5fl', 'h5mm')) for n, _ in ranked[:5]):
            recommendations.append({
                'message': 'Several of the top consumers are HDF5 free-list and memory management routines, which '
                           'points at object churn rather than I/O: reuse dataspace, datatype and property-list '
                           'identifiers instead of creating and closing them per access, and consider raising the '
                           'chunk cache so chunks are not repeatedly allocated and evicted',
                'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-cache.c'),
                                           line_numbers=True, background_color='default')})
    elif top_name.lower().startswith('mpi'):
        recommendations.append({
            'message': 'The largest consumer of library time is {} ({:.1f}% of it) -- inspect the collective '
                       'buffering and data sieving hints, which do work in library space before any syscall is '
                       'issued'.format(top_name, top_share)})
    else:
        recommendations.append({
            'message': 'Profile inside {} ({:.1f}% of library code time): the time is above the syscall layer, so '
                       'storage tuning will not reach it'.format(top_name, top_share)})

    insights_operation.append(
        message(INSIGHTS_STACK_OVERHEAD, TARGET_DEVELOPER, HIGH,
                '{:.1f}% of the time inside instrumented I/O library calls is library code rather than syscalls '
                '({:.3f} thread-seconds cumulative across {} threads, {:.3f} s per thread in a {:.3f} s run), led '
                'by {} at {:.1f}%'.format(overhead_fraction * 100, library_self, threads,
                                          library_self / threads if threads else 0.0, wall_span,
                                          top_name, top_share),
                recommendations, details)
    )


# =========================================================================
# Rebuilt versions of the remaining Darshan-era checks (per-call paths)
#
#   E12  misalignment, measured by cost rather than counted
#   E13  non-blocking I/O, judged by achieved overlap rather than usage
#   E14  MPI-IO aggregators, identified from the trace rather than hints
#   E15  redundant traffic, measured by interval union rather than inferred
#   E16  metadata, categorized and checked for contention bursts
# =========================================================================

INSIGHTS_MISALIGNMENT_COST = 'E12'
INSIGHTS_MISALIGNMENT_NO_COST = 'E12b'
INSIGHTS_NONBLOCKING_NO_OVERLAP = 'E13'
INSIGHTS_NONBLOCKING_EFFECTIVE = 'E13b'
INSIGHTS_AGGREGATORS_DETECTED = 'E14'
INSIGHTS_TRAFFIC_REDUNDANCY = 'E15'
INSIGHTS_TRAFFIC_OVERWRITE = 'E17'
INSIGHTS_METADATA_CATEGORIES = 'E16'
INSIGHTS_METADATA_CONTENTION = 'E16b'

thresholds['alignment_boundary'] = [4096, False]          # bytes; the unit a partial access must be filled to
thresholds['misalign_time_fraction'] = [0.05, False]      # measured misalignment cost worth reporting
thresholds['overlap_effective'] = [0.5, False]            # share of a transfer that must be hidden to count as overlapped
thresholds['nonblocking_min_ops'] = [5, False]            # non-blocking calls needed before judging overlap
thresholds['redundancy_factor'] = [1.5, False]            # bytes read / bytes covered before it is redundancy
thresholds['metadata_burst_gap'] = [0.01, False]          # gap (s) separating metadata bursts
thresholds['metadata_contention_ratio'] = [3.0, False]    # latency growth under concurrency that indicates serialization

NONBLOCKING_MARKERS = ('iread', 'iwrite', '_begin')
COMPLETION_MARKERS = ('wait', 'test', '_end')

METADATA_CATEGORIES = {
    'namespace': ('open', 'openat', 'openat2', 'open64', 'creat', 'stat', 'fstat', 'lstat',
                  'newfstatat', 'statx', 'access', 'faccessat', 'unlink', 'unlinkat', 'rename',
                  'renameat', 'mkdir', 'rmdir', 'getdents', 'getdents64', 'readlink', 'chmod',
                  'chown', 'truncate', 'ftruncate'),
    'sync': ('fsync', 'fdatasync', 'sync', 'syncfs', 'msync'),
    'lock': ('flock', 'fcntl', 'fcntl64'),
    'seek': ('lseek', 'llseek', '_llseek'),
    'close': ('close', 'close_range'),
}

DATA_TRANSFER_FUNCTIONS = ('read', 'write', 'pread', 'pwrite', 'pread64', 'pwrite64',
                           'readv', 'writev', 'preadv', 'pwritev')


def _categorize_metadata(function):
    for category, names in METADATA_CATEGORIES.items():
        if function in names:
            return category
    return None


def byte_union(df):
    """
    Total distinct bytes covered by a set of [offset, offset+size)
    requests, via a single sweep over sorted intervals.
    """
    if df is None or not len(df):
        return 0

    ordered = df[['offset', 'size']].sort_values('offset').values

    covered = 0
    current_start = float(ordered[0][0])
    current_end = current_start + float(ordered[0][1])

    for offset, size in ordered[1:]:
        offset = float(offset)
        end = offset + float(size)
        if offset <= current_end:
            current_end = max(current_end, end)
        else:
            covered += current_end - current_start
            current_start, current_end = offset, end

    covered += current_end - current_start
    return int(covered)


# -------------------------------------------------------------------------
# E12 -- misalignment measured by cost
# -------------------------------------------------------------------------

def check_misaligned_cost(df_posix, file_map=None):
    """
    The Darshan-era check counts requests whose offset is not on a block
    boundary. That is a property of the REQUEST, not of what the storage
    system had to do: a misaligned access whose surrounding block is
    already cached costs nothing.

    Without kernel probes the honest proxy is the trace's own timing.
    Within each size bucket, compare the median latency of aligned and
    misaligned requests. If misaligned requests are systematically slower,
    the difference is the measured cost of misalignment; if they are not,
    the misalignment is real but free and is reported as such rather than
    raised as an issue.
    """
    if df_posix is None or not len(df_posix):
        return

    boundary = thresholds['alignment_boundary'][0]

    df = df_posix[(df_posix['size'] > 0) & (df_posix['duration'] > 0)].copy()
    if not len(df):
        return

    df['misaligned'] = (df['offset'] % boundary) != 0

    # A request also costs extra when misalignment pushes it across one
    # more boundary than its size alone would require.
    blocks_if_aligned = ((df['size'] + boundary - 1) // boundary)
    blocks_actual = (((df['offset'] % boundary) + df['size'] + boundary - 1) // boundary)
    df['extra_block'] = (blocks_actual > blocks_if_aligned)

    total_ops = len(df)
    misaligned_ops = int(df['misaligned'].sum())
    if not misaligned_ops:
        return

    extra_block_ops = int(df['extra_block'].sum())
    total_time = float(df['duration'].sum())

    # Cost per size bucket: how much slower is a misaligned request than
    # an aligned one of comparable size?
    df['bucket'] = df['size'].map(lambda s: int(math.floor(math.log2(s))) if s > 0 else 0)

    measured_cost = 0.0
    compared_buckets = 0
    for bucket, group in df.groupby('bucket'):
        aligned = group[~group['misaligned']]
        misaligned = group[group['misaligned']]
        if len(aligned) < 5 or len(misaligned) < 5:
            continue
        compared_buckets += 1
        delta = float(misaligned['duration'].median() - aligned['duration'].median())
        if delta > 0:
            measured_cost += delta * len(misaligned)

    details = [
        {'message': '{} of {} requests ({:.1f}%) start off a {} boundary'.format(
            misaligned_ops, total_ops, misaligned_ops / total_ops * 100, convert_bytes(boundary))},
        {'message': '{} request(s) cross one more {} boundary than their size alone requires'.format(
            extra_block_ops, convert_bytes(boundary))},
    ]

    if not compared_buckets:
        details.append({'message': 'no size bucket held enough aligned and misaligned requests to compare their '
                                   'latencies, so the cost of the misalignment could not be measured from this '
                                   'trace'})
        insights_operation.append(
            message(INSIGHTS_MISALIGNMENT_COST, TARGET_DEVELOPER, WARN,
                    '{:.1f}% of requests are misaligned, but the trace does not allow measuring what that '
                    'costs'.format(misaligned_ops / total_ops * 100),
                    [{'message': 'Align requests to the file system block boundary, or confirm the cost first with '
                                 'page-cache tracing (mm_filemap_add_to_page_cache) before restructuring'}],
                    details)
        )
        return

    cost_fraction = measured_cost / total_time if total_time > 0 else 0.0
    details.append({'message': 'comparing aligned and misaligned requests of the same size across {} size '
                               'bucket(s), misalignment accounts for {:.3f} s ({:.1f}% of I/O time)'.format(
                                   compared_buckets, measured_cost, cost_fraction * 100)})

    if cost_fraction < thresholds['misalign_time_fraction'][0]:
        insights_operation.append(
            message(INSIGHTS_MISALIGNMENT_NO_COST, TARGET_DEVELOPER, OK,
                    '{:.1f}% of requests are misaligned but they are not measurably slower than aligned requests '
                    'of the same size -- most are being absorbed by the page cache and realigning them would '
                    'recover little'.format(misaligned_ops / total_ops * 100),
                    None, details)
        )
        return

    thresholds['misalign_time_fraction'][1] = True

    recommendations = [
        {'message': 'Align requests to the {} boundary so a partial access does not have to fill the surrounding '
                    'block'.format(convert_bytes(boundary))},
    ]
    if 'H5F' in (file_map or {}) or any(str(f).endswith(('.h5', '.hdf5')) for f in (file_map or {}).values()):
        recommendations.append({
            'message': 'Since the application uses HDF5, set the alignment in the file access property list with '
                       'H5Pset_alignment()',
            'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/hdf5-alignment.c'),
                                       line_numbers=True, background_color='default')})

    insights_operation.append(
        message(INSIGHTS_MISALIGNMENT_COST, TARGET_DEVELOPER, HIGH,
                'Misaligned requests are measurably slower than aligned requests of the same size, costing '
                '{:.3f} s ({:.1f}% of I/O time)'.format(measured_cost, cost_fraction * 100),
                recommendations, details)
    )


# -------------------------------------------------------------------------
# E13 -- non-blocking I/O judged by achieved overlap
# -------------------------------------------------------------------------

def pair_nonblocking_operations(df_intervals, df_layers):
    """
    Pair each non-blocking call with the completion call that ends it.

    MPI returns an opaque request handle that the trace does not carry, so
    pairing is by program order per thread: a wait/test takes the oldest
    outstanding request, and a waitall takes every outstanding request.
    Split collectives (_begin/_end) pair by name.
    """
    frames = []
    for df in (df_intervals, df_layers):
        if df is not None and len(df):
            cols = [c for c in ('tid', 'rank', 'function', 'start', 'end', 'duration') if c in df.columns]
            frames.append(df[cols])

    if not frames:
        return pd.DataFrame()

    events = pd.concat(frames, ignore_index=True).drop_duplicates()
    if not len(events):
        return pd.DataFrame()

    lowered = events['function'].str.lower()
    events = events[lowered.str.contains('|'.join(NONBLOCKING_MARKERS)) |
                    lowered.str.contains('|'.join(COMPLETION_MARKERS))]
    if not len(events):
        return pd.DataFrame()

    pairs = []
    for tid, group in events.groupby('tid'):
        outstanding = []
        for row in group.sort_values('start').itertuples(index=False):
            name = row.function.lower()

            is_completion = any(m in name for m in COMPLETION_MARKERS)
            is_issue = any(m in name for m in NONBLOCKING_MARKERS) and not is_completion

            if is_issue:
                outstanding.append(row)
            elif is_completion and outstanding:
                taken = outstanding if 'all' in name else [outstanding[0]]
                outstanding = [] if 'all' in name else outstanding[1:]
                for issue in taken:
                    pairs.append({
                        'tid': tid,
                        'rank': getattr(issue, 'rank', None),
                        'issue': issue.function,
                        'completion': row.function,
                        'overlap': max(0.0, float(row.start - issue.end)),
                        'wait': float(row.duration),
                    })

    return pd.DataFrame(pairs)


def check_nonblocking_overlap(df_intervals, df_layers):
    """
    The Darshan-era check only fires when NO non-blocking call was used,
    and treats any usage as correct. That misses the common case: a call
    issued and immediately waited on, which is asynchronous in syntax and
    blocking in fact. With per-call timestamps the actual overlap is
    measurable.
    """
    pairs = pair_nonblocking_operations(df_intervals, df_layers)
    if not len(pairs) or len(pairs) < thresholds['nonblocking_min_ops'][0]:
        return

    pairs = pairs.copy()
    denominator = (pairs['overlap'] + pairs['wait']).replace(0, float('nan'))
    pairs['effectiveness'] = (pairs['overlap'] / denominator).fillna(0.0)

    total_wait = float(pairs['wait'].sum())
    total_overlap = float(pairs['overlap'].sum())
    ineffective = pairs[pairs['effectiveness'] < thresholds['overlap_effective'][0]]

    details = [
        {'message': '{} non-blocking operation(s) paired with their completion call'.format(len(pairs))},
        {'message': '{:.3f} s spent between issue and wait (available for overlap) versus {:.3f} s blocked inside '
                    'the wait itself'.format(total_overlap, total_wait)},
    ]

    top = pairs.groupby('issue').agg(count=('wait', 'size'), wait=('wait', 'sum'),
                                     overlap=('overlap', 'sum')).sort_values('wait', ascending=False)
    for name, row in top.head(3).iterrows():
        details.append({'message': '{}: {} call(s), {:.3f} s blocked in wait, {:.3f} s overlapped'.format(
            name, int(row['count']), row['wait'], row['overlap'])})

    if not len(ineffective):
        insights_operation.append(
            message(INSIGHTS_NONBLOCKING_EFFECTIVE, TARGET_DEVELOPER, OK,
                    'Non-blocking operations are achieving real overlap: {:.3f} s of transfer time is hidden behind '
                    'other work'.format(total_overlap),
                    None, details)
        )
        return

    thresholds['overlap_effective'][1] = True

    recovered = float(ineffective['wait'].sum())

    recommendations = [
        {'message': 'Move the wait later, or issue the request earlier, so the transfer runs during work that does '
                    'not depend on it -- as written, {} of the {} non-blocking calls block for longer than they '
                    'overlap'.format(len(ineffective), len(pairs))},
        {'message': 'If the calls cannot be separated further, confirm the MPI stack makes progress in the '
                    'background: without an asynchronous progress thread or hardware offload, the transfer does '
                    'not start until the wait is entered, and no placement of the wait will help'},
    ]

    insights_operation.append(
        message(INSIGHTS_NONBLOCKING_NO_OVERLAP, TARGET_DEVELOPER, HIGH,
                '{} of {} non-blocking operations achieve little overlap and block for {:.3f} s inside their '
                'completion call -- they are asynchronous in form but not in effect'.format(
                    len(ineffective), len(pairs), recovered),
                recommendations, details)
    )


# -------------------------------------------------------------------------
# E14 -- aggregators identified from the trace
# -------------------------------------------------------------------------

def check_mpi_aggregators(df_intervals, df_posix_records, file_map):
    """
    The Darshan-era check compares the cb_nodes hint against the node
    count and never verifies what happened. The trace answers directly:
    during a collective call, only the aggregator ranks issue POSIX I/O
    to the file, so the set of ranks that do is the aggregator set that
    ROMIO actually chose -- which can differ from the hint.
    """
    if df_intervals is None or not len(df_intervals) or not file_map:
        return
    if df_posix_records is None or not len(df_posix_records):
        return

    df_mpiio = df_intervals[df_intervals['api'] == 'MPI-IO']
    if not len(df_mpiio):
        return

    df_coll = df_mpiio[df_mpiio['function'].map(is_collective_call)]
    if not len(df_coll):
        return

    for fid, coll_on_file in df_coll.groupby('file_id'):
        fname = file_map.get(fid)
        if fname is None:
            continue

        participants = coll_on_file['rank'].nunique()
        if participants < 2:
            continue

        _, windows = merge_intervals(coll_on_file)
        if not windows:
            continue

        starts = [w[0] for w in windows]
        ends = [w[1] for w in windows]

        posix_on_file = df_posix_records[
            (df_posix_records['fname'] == fname)
            & (df_posix_records['function'].isin(DATA_TRANSFER_FUNCTIONS))
        ]
        if not len(posix_on_file):
            continue

        idx = pd.Series(starts).searchsorted(posix_on_file['start'].values, side='right') - 1
        inside = []
        for position, call_start in zip(idx, posix_on_file['start'].values):
            inside.append(position >= 0 and call_start < ends[position])

        during = posix_on_file[pd.Series(inside, index=posix_on_file.index)]
        if not len(during):
            continue

        by_rank = during.groupby('rank')['duration'].agg(['size', 'sum'])
        aggregators = len(by_rank)

        details = [
            {'message': '{} rank(s) take part in the collective calls on this file'.format(participants)},
            {'message': '{} rank(s) actually issue POSIX I/O during those calls -- these are the aggregators ROMIO '
                        'selected, whatever cb_nodes was set to'.format(aggregators)},
            {'message': 'aggregators spend {:.3f} s in POSIX I/O across {} call(s) during the collective '
                        'windows'.format(float(by_rank['sum'].sum()), int(by_rank['size'].sum()))},
        ]

        busiest = by_rank['sum'].max()
        quietest = by_rank['sum'].min()
        if aggregators > 1 and busiest > 0:
            details.append({'message': 'aggregator I/O time ranges from {:.3f} s to {:.3f} s -- a spread of '
                                       '{:.0f}%'.format(quietest, busiest, (busiest - quietest) / busiest * 100)})

        recommendations = [
            {'message': 'Set cb_nodes to match the file layout: on a striped file system one aggregator per storage '
                        'target, with the file domains aligned to the stripe size, keeps each aggregator talking to '
                        'a single target',
             'sample': Syntax.from_path(os.path.join(ROOT, 'snippets/mpi-io-hints.bash'),
                                        line_numbers=True, background_color='default')},
            {'message': 'Placement cannot be checked from this trace: it carries process ids but no host mapping, so '
                        'whether two aggregators share a node (and therefore a network interface) is unknown. Add a '
                        'pid-to-host record to the trace to make cb_config_list advice possible'},
        ]

        insights_operation.append(
            message(INSIGHTS_AGGREGATORS_DETECTED, TARGET_DEVELOPER, INFO,
                    '{} of {} ranks act as aggregators for the collective calls on {}'.format(
                        aggregators, participants, fname),
                    recommendations, details)
        )


# -------------------------------------------------------------------------
# E15 -- redundant traffic measured, not inferred
# -------------------------------------------------------------------------

def redundant_regions(df):
    """
    Sweep the [offset, offset+size) requests and return the byte ranges
    that were touched more than once, as (start, end, times_touched).
    One pass over sorted boundaries, so it scales with request count
    rather than with pairwise comparisons.
    """
    if df is None or not len(df):
        return []

    deltas = {}
    for offset, size in df[['offset', 'size']].values:
        offset = int(offset)
        end = offset + int(size)
        if end <= offset:
            continue
        deltas[offset] = deltas.get(offset, 0) + 1
        deltas[end] = deltas.get(end, 0) - 1

    regions = []
    depth = 0
    previous = None

    for position in sorted(deltas):
        if previous is not None and position > previous and depth >= 2:
            regions.append((previous, position, depth))
        depth += deltas[position]
        previous = position

    # Merge neighbouring ranges that were touched the same number of times
    merged = []
    for region_start, region_end, times in regions:
        if merged and merged[-1][1] == region_start and merged[-1][2] == times:
            merged[-1] = (merged[-1][0], region_end, times)
        else:
            merged.append((region_start, region_end, times))

    return merged


def _analyze_redundancy(df_ops):
    """
    Measure how much of a file's traffic moved data the application
    already had. Returns None when there is nothing to report.
    """
    if df_ops is None or len(df_ops) < 2:
        return None

    total_bytes = int(df_ops['size'].sum())
    covered = byte_union(df_ops)
    if not covered or not total_bytes:
        return None

    per_rank_covered = sum(byte_union(group) for _, group in df_ops.groupby('rank'))
    if not per_rank_covered:
        return None

    op_time = float(df_ops['duration'].sum())
    redundancy = total_bytes / covered

    return {
        'total_bytes': total_bytes,
        'covered': covered,
        'redundancy': redundancy,
        'intra_rank': total_bytes / per_rank_covered,
        'cross_rank': per_rank_covered / covered,
        'n_ranks': int(df_ops['rank'].nunique()),
        'op_time': op_time,
        'wasted_time': op_time * (1 - 1 / redundancy),
        'wasted_bytes': total_bytes - covered,
    }


def _redundancy_region_details(df_ops, limit=10):
    """
    Per-region breakdown for --redundant-details: which byte ranges were
    touched repeatedly, how many times, and by which ranks.
    """
    regions = redundant_regions(df_ops)
    if not regions:
        return [], 0

    ranked = sorted(regions, key=lambda r: (r[1] - r[0]) * (r[2] - 1), reverse=True)

    details = []
    for region_start, region_end, times in ranked[:limit]:
        overlapping = df_ops[(df_ops['offset'] < region_end)
                             & ((df_ops['offset'] + df_ops['size']) > region_start)]
        ranks = sorted(overlapping['rank'].unique())
        rank_text = ', '.join(str(r) for r in ranks[:8])
        if len(ranks) > 8:
            rank_text += ' and {} more'.format(len(ranks) - 8)

        details.append({'message': 'bytes {}-{} ({}) touched {} times by rank(s) {} -- {} redundant'.format(
            region_start, region_end, convert_bytes(region_end - region_start), times, rank_text,
            convert_bytes((region_end - region_start) * (times - 1)))})

    return details, len(regions)


def check_traffic_redundancy(df_posix, file_map):
    """
    The Darshan-era check compares the highest offset touched against the
    total bytes moved, because counters are all it has. With per-call
    offsets the redundancy can be measured exactly: the union of the
    request intervals is the distinct data, and everything beyond it was
    moved more than once.

    The decomposition matters more than the number, because the fix
    differs: one rank re-reading its own data is an application caching
    problem, while many ranks reading the same data is a read-and-
    broadcast opportunity. On the write side the same measurement gives
    the overwrite factor -- how often the run rewrote bytes it had
    already written.

    By default the findings are aggregated across files. Pass
    --redundant-details to get every redundantly accessed region instead.
    """
    if df_posix is None or not len(df_posix) or not file_map:
        return

    detailed = getattr(args, 'redundant_details', False)

    for direction, is_read in (('read', True), ('write', False)):
        mask = df_posix['function'].str.contains('read')
        df_dir = df_posix[mask] if is_read else df_posix[~mask]
        if not len(df_dir):
            continue

        findings = []
        for fid, ops in df_dir.groupby('file_id'):
            fname = file_map.get(fid)
            if fname is None:
                continue
            facts = _analyze_redundancy(ops)
            if facts is None or facts['redundancy'] < thresholds['redundancy_factor'][0]:
                continue
            facts['fname'] = fname
            facts['ops'] = ops
            findings.append(facts)

        if not findings:
            continue

        thresholds['redundancy_factor'][1] = True
        findings.sort(key=lambda f: f['wasted_time'], reverse=True)

        code = INSIGHTS_TRAFFIC_REDUNDANCY if is_read else INSIGHTS_TRAFFIC_OVERWRITE

        if detailed:
            for facts in findings:
                details = [
                    {'message': '{} {} from {} of distinct data -- every byte was {} {:.1f} times on '
                                'average'.format(convert_bytes(facts['total_bytes']),
                                                 'read' if is_read else 'written',
                                                 convert_bytes(facts['covered']),
                                                 'read' if is_read else 'written',
                                                 facts['redundancy'])},
                    {'message': 'of that, {:.1f}x comes from the same rank repeating its own accesses and {:.1f}x '
                                'from different ranks touching the same bytes'.format(
                                    facts['intra_rank'], facts['cross_rank'])},
                    {'message': 'approximately {:.3f} s of the {:.3f} s spent on this file moved data the '
                                'application already had'.format(facts['wasted_time'], facts['op_time'])},
                ]

                region_details, region_count = _redundancy_region_details(facts['ops'])
                if region_details:
                    details.append({'message': '{} redundantly accessed region(s); the largest are:'.format(
                        region_count)})
                    details.extend(region_details)

                insights_operation.append(
                    message(code, TARGET_DEVELOPER, WARN,
                            'Redundant {} traffic on {}: {} {} to {} {} of distinct data'.format(
                                direction, facts['fname'], convert_bytes(facts['total_bytes']),
                                'read' if is_read else 'written',
                                'obtain' if is_read else 'store',
                                convert_bytes(facts['covered'])),
                            _redundancy_recommendations(facts, is_read), details)
                )
            continue

        # Aggregated view (default)
        total_bytes = sum(f['total_bytes'] for f in findings)
        total_covered = sum(f['covered'] for f in findings)
        total_wasted_time = sum(f['wasted_time'] for f in findings)
        total_wasted_bytes = sum(f['wasted_bytes'] for f in findings)
        overall = total_bytes / total_covered if total_covered else 1.0

        details = [
            {'message': '{} file(s) affected: {} {} to {} {} of distinct data ({:.1f}x)'.format(
                len(findings), convert_bytes(total_bytes), 'read' if is_read else 'written',
                'obtain' if is_read else 'store', convert_bytes(total_covered), overall)},
            {'message': 'approximately {:.3f} s and {} of traffic moved data the application already had'.format(
                total_wasted_time, convert_bytes(total_wasted_bytes))},
        ]

        for facts in findings[:3]:
            details.append({'message': '{}: {:.1f}x ({:.1f}x within a rank, {:.1f}x across {} ranks), '
                                       '{:.3f} s wasted'.format(
                                           facts['fname'], facts['redundancy'], facts['intra_rank'],
                                           facts['cross_rank'], facts['n_ranks'], facts['wasted_time'])})

        if len(findings) > 3:
            details.append({'message': '{} further file(s) not shown -- rerun with --redundant-details for every '
                                       'redundant region'.format(len(findings) - 3)})
        else:
            details.append({'message': 'rerun with --redundant-details to list every redundant region'})

        insights_operation.append(
            message(code, TARGET_DEVELOPER, WARN,
                    'Redundant {} traffic across {} file(s): {} {} to {} {} of distinct data, wasting '
                    '{:.3f} s'.format(direction, len(findings), convert_bytes(total_bytes),
                                      'read' if is_read else 'written', 'obtain' if is_read else 'store',
                                      convert_bytes(total_covered), total_wasted_time),
                    _redundancy_recommendations(findings[0], is_read), details)
        )


def _redundancy_recommendations(facts, is_read):
    if not is_read:
        return [
            {'message': 'The same bytes are written repeatedly: write each region once per checkpoint instead of '
                        'rewriting it, or buffer the updates in memory and flush the final value'},
        ]

    if facts['cross_rank'] >= facts['intra_rank'] and facts['n_ranks'] > 1:
        return [
            {'message': 'The redundancy is mostly across ranks: have one rank read the shared region and broadcast '
                        'it (MPI_Bcast) instead of {} ranks each reading it, which removes about {} of file system '
                        'traffic'.format(facts['n_ranks'], convert_bytes(facts['wasted_bytes']))},
        ]

    return [
        {'message': 'The redundancy is mostly within a rank: cache the region in memory after the first read '
                    'instead of returning to the file system for data the rank already holds'},
    ]


# -------------------------------------------------------------------------
# E16 -- metadata categorized, and checked for contention
# -------------------------------------------------------------------------

def check_metadata_categories(df_posix_records):
    """
    The Darshan-era check reports a count of ranks whose metadata time
    exceeds a fixed number of seconds -- and on the per-call paths it was
    summing every POSIX call, data transfers included.

    Categorizing the calls matters because the fixes are unrelated:
    namespace operations are server round trips, sync operations are data
    flushes wearing a metadata costume, and lock operations are
    contention. Concurrency is measured too: when latency grows with the
    number of calls in flight, the server is serializing them.
    """
    if df_posix_records is None or not len(df_posix_records):
        return

    df = df_posix_records.copy()
    df['category'] = df['function'].map(_categorize_metadata)

    df_meta = df[df['category'].notna()]
    if not len(df_meta):
        return

    df_data = df[df['function'].isin(DATA_TRANSFER_FUNCTIONS)]

    metadata_time = float(df_meta['duration'].sum())
    data_time = float(df_data['duration'].sum())
    total_time = metadata_time + data_time
    if total_time <= 0:
        return

    metadata_share = metadata_time / total_time

    by_category = df_meta.groupby('category')['duration'].agg(['size', 'sum']).sort_values('sum', ascending=False)

    details = [
        {'message': 'metadata accounts for {:.3f} s of the {:.3f} s spent in I/O calls ({:.1f}%)'.format(
            metadata_time, total_time, metadata_share * 100)},
    ]
    for category, row in by_category.iterrows():
        details.append({'message': '{}: {} call(s), {:.3f} s ({:.1f}% of metadata time)'.format(
            category, int(row['size']), row['sum'], row['sum'] / metadata_time * 100 if metadata_time else 0)})

    _, bursts = merge_intervals(df_meta, gap=thresholds['metadata_burst_gap'][0])
    if bursts:
        longest = max(bursts, key=lambda b: b[1] - b[0])
        details.append({'message': 'metadata arrives in {} burst(s); the longest runs {:.3f} s'.format(
            len(bursts), longest[1] - longest[0])})

    # Concurrency sweep: how many metadata calls were in flight when each
    # one started, and does latency grow with that number?
    starts = df_meta['start'].sort_values().values
    ends = df_meta['end'].sort_values().values
    depth = (pd.Series(starts).searchsorted(df_meta['start'].values, side='right')
             - pd.Series(ends).searchsorted(df_meta['start'].values, side='right'))
    df_meta = df_meta.assign(depth=depth)

    # Compare per-depth medians rather than splitting the calls at the
    # median depth: a storm puts most calls at one high depth, which would
    # leave the "deep" side of such a split empty.
    contention_ratio = None
    by_depth = df_meta.groupby('depth')['duration'].agg(['size', 'median'])
    by_depth = by_depth[by_depth['size'] >= 5]

    if len(by_depth) >= 2:
        shallowest = by_depth.index.min()
        deepest = by_depth.index.max()
        base = by_depth.loc[shallowest, 'median']
        peak = by_depth.loc[deepest, 'median']
        if base > 0 and deepest > shallowest:
            contention_ratio = float(peak / base)
            details.append({'message': 'metadata latency rises {:.1f}x with concurrency: median {:.6f} s with {} '
                                       'call(s) in flight versus {:.6f} s with {}'.format(
                                           contention_ratio, base, int(shallowest), peak, int(deepest))})

    if contention_ratio and contention_ratio >= thresholds['metadata_contention_ratio'][0]:
        thresholds['metadata_contention_ratio'][1] = True
        top_category = by_category.index[0]
        insights_operation.append(
            message(INSIGHTS_METADATA_CONTENTION, TARGET_DEVELOPER, HIGH,
                    'Metadata operations slow down {:.1f}x when they are issued concurrently -- the server is '
                    'serializing them, and {} operations dominate'.format(contention_ratio, top_category),
                    [{'message': 'Stagger the metadata operations or reduce their number: the cost is not per call '
                                 'but per call in flight, so the same work issued in waves will complete faster'},
                     {'message': 'If these are namespace operations on a shared directory, spread the files across '
                                 'directories, or have one rank perform the operation and share the result'}],
                    details)
        )
        return

    if metadata_share >= thresholds['metadata_time_rank'][0] / 100.0 or metadata_share >= 0.2:
        thresholds['metadata_time_rank'][1] = True
        top_category = by_category.index[0]
        insights_operation.append(
            message(INSIGHTS_METADATA_CATEGORIES, TARGET_DEVELOPER, WARN,
                    'Metadata operations consume {:.1f}% of I/O time ({:.3f} s), dominated by {} '
                    'operations'.format(metadata_share * 100, metadata_time, top_category),
                    [{'message': 'Reduce or combine the {} operations -- they are the largest metadata cost in this '
                                 'run'.format(top_category)}],
                    details)
        )
    else:
        insights_operation.append(
            message(INSIGHTS_METADATA_CATEGORIES, TARGET_DEVELOPER, OK,
                    'Metadata operations account for {:.1f}% of I/O time and show no contention signature'.format(
                        metadata_share * 100),
                    None, details)
        )
