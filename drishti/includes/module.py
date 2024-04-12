#!/usr/bin/env python3

import datetime
import csv
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


def check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules, file_map, df_posix):
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

                            if int(small_read_requests_ranks[0]) == 0 and len(small_read_requests_ranks) > 1:
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

        detail = []
        recommendation = []

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


def check_misaligned(total_operations, total_mem_not_aligned, total_file_not_aligned, modules):
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
                df_lustre = report.records['LUSTRE'].to_df()
                
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


def check_traffic(max_read_offset, total_read_size, max_write_offset, total_written_size):
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

                    redundant_ranks_ind = -1
                    temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == id]
                    updated_offsets = (temp_df["offsets"].to_numpy()).tolist()

                    for i in range(len(updated_offsets)):
                        if updated_offsets.count(updated_offsets[i]) > 1: 
                            redundant_ranks_ind = i
                            break

                    if random_ranks_ind != -1:
                        random_rank = temp_df.iloc[redundant_ranks_ind]['rank']
                        random_offsets = temp_df.iloc[redundant_ranks_ind]['offsets']

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

                    redundant_ranks_ind = -1
                    temp_df = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == id]
                    updated_offsets = (temp_df["offsets"].to_numpy()).tolist()
                    for i in range(len(updated_offsets)):
                        if updated_offsets.count(updated_offsets[i]) > 1: 
                            redundant_ranks_ind = i
                            break

                    if random_ranks_ind != -1:
                        random_rank = temp_df.iloc[redundant_ranks_ind]['rank']
                        random_offsets = temp_df.iloc[redundant_ranks_ind]['offsets']

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


def check_random_operation(read_consecutive, read_sequential, read_random, total_reads, write_consecutive, write_sequential, write_random, total_writes):
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


def check_shared_data_imblance(stragglers_count, detected_files, file_map):
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
                    temp = dxt_posix.loc[dxt_posix['id'] == int(file[0])]
                    temp_df_1 = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == int(file[0])]
                    temp_df_2 = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == int(file[0])]

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


def check_individual_write_imbalance(imbalance_count, detected_files, file_map):
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
                    temp = dxt_posix.loc[dxt_posix['id'] == int(file[0])]
                    temp_df = dxt_posix_write_data.loc[dxt_posix_write_data['id'] == int(file[0])]

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


def check_individual_read_imbalance(imbalance_count, detected_files, file_map):
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
                    temp = dxt_posix.loc[dxt_posix['id'] == int(file[0])]
                    temp_df = dxt_posix_read_data.loc[dxt_posix_read_data['id'] == int(file[0])]

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


def check_mpi_collective_read_operation(mpiio_coll_reads, mpiio_indep_reads, total_mpiio_read_operations, detected_files, file_map):
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


def check_mpi_collective_write_operation(mpiio_coll_writes, mpiio_indep_writes, total_mpiio_write_operations, detected_files, file_map):
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

def export_html(console, filename):
    '''
    '''

    if args.export_html:
        console.save_html(
            filename,
            theme=set_export_theme(),
            clear=False
        )


def export_svg(console, filename):
    if args.export_svg:
        console.save_svg(
            filename,
            title='Drishti',
            theme=set_export_theme(),
            clear=False
        )


def export_csv(filename, jobid=None):
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
        detected_issues['JOB'] = jobid

        for report in csv_report:
            detected_issues[report] = True

        with open(filename, 'w') as f:
            w = csv.writer(f)
            w.writerow(detected_issues.keys())
            w.writerow(detected_issues.values())

