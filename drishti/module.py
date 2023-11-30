#!/usr/bin/env python3

import datetime
import csv
from rich import box
from rich.syntax import Syntax
from .config import *

'''
Before calling the functions below
Make sure the variables passed are in the given structure:
file_map: a dict of (id, path) pair
modules: a set or a dict should be ok
detected_files: A pandas dataframe
'''

# Basic usage check

def check_stdio(total_size, total_size_stdio):
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


'''
detected_files required columns:
['id', 'total_reads', 'total_writes']
detected_files.loc[:, 'id'] = detected_files.loc[:, 'id'].astype(str)
'''
def check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules, file_map):
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


''''
The shared_file required columns:
['id', 'INSIGHTS_POSIX_SMALL_READS', 'INSIGHTS_POSIX_SMALL_WRITES']
'''
def check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes, total_shared_writes_small, shared_files, file_map):
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


'''
detected_files required columns:
['id', 'data_imbalance']
'''
def check_shared_data_imblance(stragglers_count, detected_files, file_map):
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


'''
detected_files required columns:
['id', 'time_imbalance']
'''
def check_shared_time_imbalance(stragglers_count, detected_files, file_map):
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


'''
detected_files required columns:
['id', 'write_imbalance']
'''
def check_individual_write_imbalance(imbalance_count, detected_files, file_map):
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


'''
detected_files required columns:
['id', 'read_imbalance']
'''
def check_individual_read_imbalance(imbalance_count, detected_files, file_map):
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


# MPIIO level check

'''
detected_files required columns:
['id', 'absolute_indep_reads', 'percent_indep_reads']
'''
def check_mpi_collective_read_operation(mpiio_coll_reads, mpiio_indep_reads, total_mpiio_read_operations, detected_files, file_map):
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


'''
detected_files required columns:
['id', 'absolute_indep_writes', 'percent_indep_writes']
'''
def check_mpi_collective_write_operation(mpiio_coll_writes, mpiio_indep_writes, total_mpiio_write_operations, detected_files, file_map):
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

def display_content():
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


def display_footer(insights_start_time, insights_end_time):
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

def export_html():
    if args.export_html:
        console.save_html(
            '{}.html'.format(args.log_path),
            theme=export_theme,
            clear=False
        )


def export_svg():
    if args.export_svg:
        console.save_svg(
            '{}.svg'.format(args.log_path),
            title='Drishti',
            theme=export_theme,
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

