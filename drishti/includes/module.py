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

    if total_writes > total_reads and total_operations and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is write operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
            total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )

    if total_reads > total_writes and total_operations and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
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

    if total_written_size > total_read_size and abs(total_written_size - total_read_size) / total_size > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is write size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
            total_written_size / total_size * 100.0, total_read_size / total_size * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )

    if total_read_size > total_written_size and abs(total_written_size - total_read_size) / total_size > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is read size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
            total_written_size / total_size * 100.0, total_read_size / total_size * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_READ_SIZE_INTENSIVE, TARGET_DEVELOPER, INFO, issue, None)
        )


def check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules, file_map):
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
    '''

    if total_reads_small and total_reads_small / total_reads > THRESHOLD_SMALL_REQUESTS and total_reads_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
        issue = 'Application issues a high number ({}) of small read requests (i.e., < 1MB) which represents {:.2f}% of all read requests'.format(
            total_reads_small, total_reads_small / total_reads * 100.0
        )

        detail = []
        recommendation = []

        for index, row in detected_files.iterrows():
            if row['total_reads'] > (total_reads * THRESHOLD_SMALL_REQUESTS / 2):
                detail.append(
                    {
                        'message': '{} ({:.2f}%) small read requests are to "{}"'.format(
                            row['total_reads'],
                            row['total_reads'] / total_reads * 100.0,
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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
            message(INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )

    if total_writes_small and total_writes_small / total_writes > THRESHOLD_SMALL_REQUESTS and total_writes_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
        issue = 'Application issues a high number ({}) of small write requests (i.e., < 1MB) which represents {:.2f}% of all write requests'.format(
            total_writes_small, total_writes_small / total_writes * 100.0
        )

        detail = []
        recommendation = []

        for index, row in detected_files.iterrows():
            if row['total_writes'] > (total_writes * THRESHOLD_SMALL_REQUESTS / 2):
                detail.append(
                    {
                        'message': '{} ({:.2f}%) small write requests are to "{}"'.format(
                            row['total_writes'],
                            row['total_writes'] / total_writes * 100.0,
                            file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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

    if total_operations and total_mem_not_aligned / total_operations > THRESHOLD_MISALIGNED_REQUESTS:
        issue = 'Application has a high number ({:.2f}%) of misaligned memory requests'.format(
            total_mem_not_aligned / total_operations * 100.0
        )

        insights_metadata.append(
            message(INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE, TARGET_DEVELOPER, HIGH, issue, None)
        )

    if total_operations and total_file_not_aligned / total_operations > THRESHOLD_MISALIGNED_REQUESTS:
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

        if 'LUSTRE' in modules:
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

        insights_metadata.append(
            message(INSIGHTS_POSIX_REDUNDANT_READ_USAGE, TARGET_DEVELOPER, WARN, issue, None)
        )

    if max_write_offset > total_written_size:
        issue = 'Application might have redundant write traffic (more data written than the highest offset)'

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

    if total_shared_reads and total_shared_reads_small / total_shared_reads > THRESHOLD_SMALL_REQUESTS and total_shared_reads_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
        issue = 'Application issues a high number ({}) of small read requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file read requests'.format(
            total_shared_reads_small, total_shared_reads_small / total_shared_reads * 100.0
        )

        detail = []

        for index, row in shared_files.iterrows():
            if row['INSIGHTS_POSIX_SMALL_READS'] > (total_shared_reads * THRESHOLD_SMALL_REQUESTS / 2):
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

    if total_shared_writes and total_shared_writes_small / total_shared_writes > THRESHOLD_SMALL_REQUESTS and total_shared_writes_small > THRESHOLD_SMALL_REQUESTS_ABSOLUTE:
        issue = 'Application issues a high number ({}) of small write requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file write requests'.format(
            total_shared_writes_small, total_shared_writes_small / total_shared_writes * 100.0
        )

        detail = []

        for index, row in shared_files.iterrows():
            if row['INSIGHTS_POSIX_SMALL_WRITES'] > (total_shared_writes * THRESHOLD_SMALL_REQUESTS / 2):
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
        issue = 'There are {} ranks where metadata operations take over {} seconds'.format(
            count_long_metadata, THRESHOLD_METADATA_TIME_RANK
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
        issue = 'Detected data transfer imbalance caused by stragglers when accessing {} shared file.'.format(
            stragglers_count
        )

        detail = []

        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['data_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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

    if total_transfer_size and abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size > THRESHOLD_STRAGGLERS:
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

    if total_transfer_time and abs(slowest_rank_time - fastest_rank_time) / total_transfer_time > THRESHOLD_STRAGGLERS:
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
        issue = 'Detected write imbalance when accessing {} individual files'.format(
            imbalance_count
        )

        detail = []
        
        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['write_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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
            message(INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )


def check_individual_write_imbalance_split(max_bytes_written, min_bytes_written):
    '''
    Check whether there is write imbalance in the specific individual file

    Parameters:
        max_bytes_written: max byte written in the file
        min_bytes_written: minimum byte written in the file
    '''

    if max_bytes_written and abs(max_bytes_written - min_bytes_written) / max_bytes_written > THRESHOLD_IMBALANCE:
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
        issue = 'Detected read imbalance when accessing {} individual files.'.format(
            imbalance_count
        )

        detail = []
        
        for index, row in detected_files.iterrows():
            detail.append(
                {
                    'message': 'Load imbalance of {:.2f}% detected while accessing "{}"'.format(
                        row['read_imbalance'],
                        file_map[int(row['id'])] if args.full_path else os.path.basename(file_map[int(row['id'])])
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
            message(INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE, TARGET_DEVELOPER, HIGH, issue, recommendation, detail)
        )


def check_individual_read_imbalance_split(max_bytes_read, min_bytes_read):
    '''
    Check whether there is read imbalance in the specific individual file

    Parameters:
        max_bytes_written: max byte read in the file
        min_bytes_written: minimum byte read in the file
    '''

    if max_bytes_read and abs(max_bytes_read - min_bytes_read) / max_bytes_read > THRESHOLD_IMBALANCE:
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
        if total_mpiio_read_operations and total_mpiio_read_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
            issue = 'Application uses MPI-IO but it does not use collective read operations, instead it issues {} ({:.2f}%) independent read calls'.format(
                mpiio_indep_reads,
                mpiio_indep_reads / total_mpiio_read_operations * 100
            )

            detail = []

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
        if total_mpiio_write_operations and total_mpiio_write_operations > THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE:
            issue = 'Application uses MPI-IO but it does not use collective write operations, instead it issues {} ({:.2f}%) independent write calls'.format(
                mpiio_indep_writes,
                mpiio_indep_writes / total_mpiio_write_operations * 100
            )

            detail = []

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

