import os
import sys
import time
import datetime
import argparse

import darshan
import darshan.backend.cffi_backend as darshanll

from rich import print, box, rule
from rich.console import Console, Group
from rich.padding import Padding
from rich.text import Text
from rich.syntax import Syntax
from rich.panel import Panel
from subprocess import call

console = Console(record=True)

RECOMMENDATIONS = 0
HIGH = 1
WARN = 2
INFO = 3
OK = 4

TARGET_USER = 1
TARGET_DEVELOPER = 2
TARGET_SYSTEM = 3

insights_operation = []
insights_metadata = []

insights_total = dict()

insights_total[HIGH] = 0
insights_total[WARN] = 0
insights_total[RECOMMENDATIONS] = 0

THRESHOLD_OPERATION_IMBALANCE = 0.1
THRESHOLD_SMALL_REQUESTS = 0.1
THRESHOLD_MISALIGNED_REQUESTS = 0.1
THRESHOLD_METADATA = 0.1
THRESHOLD_METADATA_TIME_RANK = 30  # seconds
THRESHOLD_RANDOM_OPERATIONS = 0.2
THRESHOLD_STRAGGLERS = 0.15
THRESHOLD_INTERFACE_STDIO = 0.1

NUMBER_OF_COMPUTE_NODES = 32

# TODO: need to verify the threashold to be between 0 and 1
# TODO: read thresholds from file


# It would be benneficial to have the number of nodes alongsize the darshan record
filename = sys.argv[1]

if not os.path.isfile(filename):
    print('Unable to open .darshan file.')

    sys.exit(os.EX_NOINPUT)

parser = argparse.ArgumentParser(
    description='I/O Insights: '
)

parser.add_argument(
    'darshan',
    help='Input .darshan file'
)

parser.add_argument(
    '--issues',
    default=False,
    action='store_true',
    dest='only_issues',
    help='Only displays the detected issues and hides the recommendations'
)

parser.add_argument(
    '--export',
    default=False,
    action='store_true',
    dest='export_html',
    help='Export the report as an HTML page'
)

parser.add_argument(
    '--verbose',
    default=False,
    action='store_true',
    dest='verbose',
    help='Display extended details for the recommendations'
)

args = parser.parse_args()


def validate_thresholds():
    """
    Validate thresholds defined by the user.
    """
    assert(THRESHOLD_OPERATION_IMBALANCE >= 0.0 and THRESHOLD_OPERATION_IMBALANCE <= 1.0)
    assert(THRESHOLD_SMALL_REQUESTS >= 0.0 and THRESHOLD_SMALL_REQUESTS <= 1.0)
    assert(THRESHOLD_MISALIGNED_REQUESTS >= 0.0 and THRESHOLD_MISALIGNED_REQUESTS <= 1.0)
    assert(THRESHOLD_METADATA >= 0.0 and THRESHOLD_METADATA <= 1.0)
    assert(THRESHOLD_RANDOM_OPERATIONS >= 0.0 and THRESHOLD_RANDOM_OPERATIONS <= 1.0)

    assert(THRESHOLD_METADATA_TIME_RANK >= 0.0)


def clear():
    # check and make call for specific operating system
    _ = call('clear' if os.name == 'posix' else 'cls')


def convert_bytes(bytes_number):
    """
    Convert bytes into formatted string.
    """
    tags = [
        'bytes',
        'KB',
        'MB',
        'GB',
        'TB',
        'PB',
        'EB'
    ]

    i = 0
    double_bytes = bytes_number

    while (i < len(tags) and  bytes_number >= 1024):
        double_bytes = bytes_number / 1024.0
        i = i + 1
        bytes_number = bytes_number / 1024

    return str(round(double_bytes, 2)) + ' ' + tags[i]


def message(target, level, issue, recommendations=None):
    """
    Display the message on the screen with level, issue, and recommendation.
    """
    icon = ':arrow_forward:'

    if level in (HIGH, WARN):
        insights_total[level] += 1

    if level == HIGH:
        color = '[red]'
    elif level == WARN:
        color = '[orange1]'
    elif level == OK:
        color = '[green]'
    else:
        color = ''

    messages = [
        ' {}{} {}'.format(
            color,
            icon,
            issue
        )
    ]

    if recommendations:
        if not args.only_issues:
            messages.append('   [white]:left_arrow_curving_right: [b]Recommendations:[/b]')

            for recommendation in recommendations:
                messages.append('     :left_arrow_curving_right: {}'.format(recommendation['message']))

                if args.verbose and 'sample' in recommendation:
                    messages.append(
                        Padding(
                            Panel(
                                recommendation['sample'],
                                title='Solution Snippet',
                                title_align='left',
                                padding=(1, 2)
                            ),
                            (1, 0, 1, 7)
                        )
                    )

        insights_total[RECOMMENDATIONS] += len(recommendations)

    return Group(
        *messages
    )

# clear()
validate_thresholds()

insights_start_time = time.time()

log = darshanll.log_open(filename)

# Access various job information
job = darshanll.log_get_job(log)

# print(job)
# Example Return:
# {'jobid': 4478544,
# 'uid': 69615,
# 'start_time': 1490000867,
# 'end_time': 1490000983,
# 'metadata': {'lib_ver': '3.1.3', 'h': 'romio_no_indep_rw=true;cb_nodes=4'}}

# Access available modules and modules
# print(darshanll.log_get_modules(log))

modules = darshanll.log_get_modules(log)

# print(modules)

# Example Return:
# {'POSIX': {'len': 186, 'ver': 3, 'idx': 1},
#  'MPI-IO': {'len': 154, 'ver': 2, 'idx': 2},
#  'LUSTRE': {'len': 87, 'ver': 1, 'idx': 6},
#  'STDIO': {'len': 3234, 'ver': 1, 'idx': 7}}

# print(darshanll.log_get_generic_record(log, 'POSIX'))

# Access different record types as numpy arrays, with integer and float counters seperated
# Example Return: {'counters': array([...], dtype=uint64), 'fcounters': array([...])}
#posix_record = darshanll.log_get_posix_record(log)
#mpiio_record = darshanll.log_get_mpiio_record(log)
#stdio_record = darshanll.log_get_stdio_record(log)
# ...

darshanll.log_close(log)

darshan.enable_experimental()

report = darshan.DarshanReport(filename)
#report.info()

# TEMPLATE
# -----------
# Target: End-user, developer, sysadmin
# Title
# Description
# Why this is important?
# How to fix?
# Reference/sample

#report.read_all_generic_records()
#report.summarize()

#########################################################################################################################################################################

# Check usage of STDIO, POSIX, and MPI-IO

df_stdio = report.records['STDIO'].to_df()

if 'STDIO' in report.records:
    if df_stdio:
        total_write_size_stdio = df_stdio['counters']['STDIO_BYTES_WRITTEN'].sum()
        total_read_size_stdio = df_stdio['counters']['STDIO_BYTES_READ'].sum()

        total_size_stdio = total_write_size_stdio + total_read_size_stdio 
    else:
        total_size_stdio = 0
else:
    total_size_stdio = 0

if 'POSIX' in report.records:
    df_posix = report.records['POSIX'].to_df()

    if df_posix:
        total_write_size_posix = df_posix['counters']['POSIX_BYTES_WRITTEN'].sum()
        total_read_size_posix = df_posix['counters']['POSIX_BYTES_READ'].sum()

        total_size_posix = total_write_size_posix + total_read_size_posix
    else:
        total_size_posix = 0
else:
    total_size_posix = 0

if 'MPI-IO' in report.records:
    df_mpiio = report.records['MPI-IO'].to_df()

    if df_mpiio:
        total_write_size_mpiio = df_mpiio['counters']['MPIIO_BYTES_WRITTEN'].sum()
        total_read_size_mpiio = df_mpiio['counters']['MPIIO_BYTES_READ'].sum()

        total_size_mpiio = total_write_size_mpiio + total_read_size_mpiio 
    else:
        total_size_mpiio = 0
else:
    total_size_mpiio = 0

# Since POSIX will capture both POSIX-only accesses and those comming from MPI-IO, we can subtract those
total_size_posix -= total_size_mpiio

total_size = total_size_stdio + total_size_posix + total_size_mpiio

assert(total_size_stdio >= 0)
assert(total_size_posix >= 0)
assert(total_size_mpiio >= 0)

if total_size_stdio / total_size > THRESHOLD_INTERFACE_STDIO:
    issue = 'Application is using STDIO, a low-performance interface, for {:.2f}% of its data transfers ({})'.format(
        total_size_stdio / total_size * 100.0,
        convert_bytes(total_size_stdio)
    )

    recommendation = 'Consider switching to a high-performance I/O interface such as MPI-IO'

    insights_operation.append(
        message(TARGET_DEVELOPER, HIGH, issue, recommendation)
    )

if 'MPI-IO' not in modules:
    issue = 'Application is using low-performance interface'

    recommendation = [
        {
            'message': 'Consider switching to a high-performance I/O interface such as MPI-IO'
        }
    ]

    insights_operation.append(
        message(TARGET_DEVELOPER, WARN, issue, recommendation)
    )

#########################################################################################################################################################################

if 'POSIX' in report.records:
    df = report.records['POSIX'].to_df()

    #print(df)
    #print(df['counters'].columns)
    #print(df['fcounters'].columns)

    #########################################################################################################################################################################

    # Get number of write/read operations
    total_reads = df['counters']['POSIX_READS'].sum()
    total_writes = df['counters']['POSIX_WRITES'].sum()

    # Get total number of I/O operations
    total_operations = total_writes + total_reads 

    # To check whether the application is write-intersive or read-intensive we only look at the POSIX level and check if the difference between reads and writes is larger than 10% (for more or less), otherwise we assume a balance
    if total_writes > total_reads and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is write operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
            total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
        )

        insights_metadata.append(
            message(TARGET_DEVELOPER, INFO, issue, None)
        )

    if total_reads > total_writes and abs(total_writes - total_reads) / total_operations > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is read operation intensive ({:.2f}% writes vs. {:.2f}% reads)'.format(
            total_writes / total_operations * 100.0, total_reads / total_operations * 100.0
        )

        insights_metadata.append(
            message(TARGET_DEVELOPER, INFO, issue, None)
        )

    total_read_size = df['counters']['POSIX_BYTES_READ'].sum()
    total_written_size = df['counters']['POSIX_BYTES_WRITTEN'].sum()

    total_size = total_written_size + total_read_size

    if total_written_size > total_read_size and abs(total_written_size - total_read_size) / (total_written_size + total_read_size) > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is write size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
            total_written_size / (total_written_size + total_read_size) * 100.0, total_read_size / (total_written_size + total_read_size) * 100.0
        )

        insights_metadata.append(
            message(TARGET_DEVELOPER, INFO, issue, None)
        )

    if total_read_size > total_written_size and abs(total_written_size - total_read_size) / (total_written_size + total_read_size) > THRESHOLD_OPERATION_IMBALANCE:
        issue = 'Application is read size intensive ({:.2f}% write vs. {:.2f}% read)'.format(
            total_written_size / (total_written_size + total_read_size) * 100.0, total_read_size / (total_written_size + total_read_size) * 100.0
        )

        insights_metadata.append(
            message(TARGET_DEVELOPER, INFO, issue, None)
        )

    #########################################################################################################################################################################

    # Get the number of small I/O operations (less than 1 MB)
    total_reads_small = df['counters']['POSIX_SIZE_READ_0_100'].sum() + df['counters']['POSIX_SIZE_READ_1K_10K'].sum() + df['counters']['POSIX_SIZE_READ_100K_1M'].sum()

    if total_reads_small and total_reads_small / total_reads > THRESHOLD_SMALL_REQUESTS:
        issue = 'Application issues a high number ({}) of small read requests (i.e., < 1MB) which represents {:.2f}% of all requests'.format(
            total_reads_small, total_reads_small / total_reads * 100.0
        )

        recommendation = [
            {
                'message': 'Consider buffering read operations into larger more contiguous ones'
            }
        ]

        if 'MPI-IO' in modules:
            recommendation.append(
                {
                    'message': 'Since your appplication already uses MPI-IO, consider using collective I/O calls (e.g. MPI_File_read_all() or MPI_File_read_at_all()) to aggregate requests into larger ones',
                    'sample': Syntax.from_path('snippets/mpi-io-collective-read.c', line_numbers=True, background_color='default')
                }
            )
        else:
            recommendation.append(
                {
                    'message': 'Your application do not use MPI-IO for operations, consider use this interface instead to harness collective operations'
                }
            )

        insights_operation.append(
            message(TARGET_DEVELOPER, HIGH, issue, recommendation)
        )

    # Get the number of small I/O operations (less than the stripe size)
    total_writes_small = df['counters']['POSIX_SIZE_WRITE_0_100'].sum() + df['counters']['POSIX_SIZE_WRITE_1K_10K'].sum() + df['counters']['POSIX_SIZE_WRITE_100K_1M'].sum()

    if total_writes_small and total_writes_small / total_writes > THRESHOLD_SMALL_REQUESTS:
        issue = 'Application issues a high number ({}) of small write requests (i.e., < 1MB) which represents {:.2f}% of all requests'.format(
            total_writes_small, total_writes_small / total_writes * 100.0
        )

        recommendation = [
            {
                'message': 'Consider buffering read operations into larger more contiguous ones'
            }
        ]

        if 'MPI-IO' in modules:
            recommendation.append(
                {
                    'message': 'Since your appplication already uses MPI-IO, consider using collective I/O calls (e.g. MPI_File_write_all() or MPI_File_write_at_all()) to aggregate requests into larger ones',
                    'sample': Syntax.from_path('snippets/mpi-io-collective-write.c', line_numbers=True, background_color='default')
                }
            )
        else:
            recommendation.append(
                {
                    'message': 'Your application do not use MPI-IO for operations, consider use this interface instead to harness collective operations'
                }
            )

        insights_operation.append(
            message(TARGET_DEVELOPER, HIGH, issue, recommendation)
        )

    #########################################################################################################################################################################

    # How many requests are misaligned?

    total_mem_not_aligned = df['counters']['POSIX_MEM_NOT_ALIGNED'].sum()
    total_file_not_aligned = df['counters']['POSIX_FILE_NOT_ALIGNED'].sum()

    if total_mem_not_aligned / total_operations > THRESHOLD_MISALIGNED_REQUESTS:
        issue = 'Application has a high number ({:.2f}%) of misaligned memory requests'.format(
            total_mem_not_aligned / total_operations * 100.0
        )

        insights_metadata.append(
            message(TARGET_DEVELOPER, HIGH, issue, None)
        )

    if total_mem_not_aligned / total_operations > THRESHOLD_MISALIGNED_REQUESTS:
        issue = 'Application issues a high number ({:.2f}%) of misaligned file requests'.format(
            total_file_not_aligned / total_operations * 100.0
        )

        recommendation = [
            {
                'message': 'Consider aligning the requests to the file system block boundaries'
            }
        ]

        if 'HDF5' in modules:
            recommendation.append(
                {
                    'message': 'Since your appplication uses HDF5, consider using H5Pset_alignment() in a file access property list',
                    'sample': Syntax.from_path('snippets/hdf5-alignment.c', line_numbers=True, background_color='default')
                },
                {
                    'message': 'Any file object greater than or equal in size to threshold bytes will be aligned on an address which is a multiple of alignment'
                }
            )

        if 'LUSTRE' in modules:
            recommendation.append(
                {
                    'message': 'Since your application is accessing Lustre, consider using an alignment that matches the file system stripe configuration',
                    'sample': Syntax.from_path('snippets/lustre-striping.bash', line_numbers=True, background_color='default')
                }
            )

        insights_metadata.append(
            message(TARGET_DEVELOPER, HIGH, issue, recommendation)
        )

    #########################################################################################################################################################################

    #print(df['counters']['POSIX_MAX_BYTE_READ'] / df['counters']['POSIX_BYTES_READ'])
    #print(df['counters']['POSIX_MAX_BYTE_WRITTEN'] / df['counters']['POSIX_BYTES_WRITTEN'])

    #print(df['counters']['POSIX_MAX_BYTE_READ'].sum())
    #print(df['counters']['POSIX_BYTES_READ'].sum())

    #print(df['counters']['POSIX_MAX_BYTE_WRITTEN'].sum())
    #print(df['counters']['POSIX_BYTES_WRITTEN'].sum())

    #print(df['counters']['POSIX_STRIDE1_STRIDE'])

    #df_lustre = report.records['LUSTRE'].to_df()
    #print(df_lustre)

    # TODO: rank imabalance



    # Get the total transfer size of rank 0

    # Get the total transfer size for each rank

    #print(df['counters'])
    #exit()
    #########################################################################################################################################################################

    # Redundant read-traffic (based on Phill)
    # POSIX_MAX_BYTE_READ (Highest offset in the file that was read)
    max_read_offset = df['counters']['POSIX_MAX_BYTE_READ'].max()

    if max_read_offset > total_read_size:
        issue = 'Application might have redundant read traffic (more data was read than the highest read offset)'

        insights_metadata.append(
            message(TARGET_DEVELOPER, WARN, issue, None)
        )

    max_write_offset = df['counters']['POSIX_MAX_BYTE_WRITTEN'].max()

    if max_write_offset > total_written_size:
        issue = 'Application might have redundant write traffic (more data was write than the highest write offset)'

        insights_metadata.append(
            message(TARGET_DEVELOPER, WARN, issue, None)
        )

    #########################################################################################################################################################################

    # Check for a lot of random operations

    read_consecutive = df['counters']['POSIX_CONSEC_READS'].sum()
    #print('READ Consecutive: {} ({:.2f}%)'.format(read_consecutive, read_consecutive / total_reads * 100))

    read_sequential = df['counters']['POSIX_SEQ_READS'].sum()
    read_sequential -= read_consecutive
    #print('READ Sequential: {} ({:.2f}%)'.format(read_sequential, read_sequential / total_reads * 100))

    read_random = total_reads - read_consecutive - read_sequential
    #print('READ Random: {} ({:.2f}%)'.format(read_random, read_random / total_reads * 100))

    if total_reads:
        if read_random and read_random / total_reads > THRESHOLD_RANDOM_OPERATIONS:
            issue = 'Application is issuing a high number ({}) of random read operations ({:.2f}%)'.format(
                read_random, read_random / total_reads * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider changing your data model to have consecutive or sequential reads'
                }
            ]

            insights_operation.append(
                message(TARGET_DEVELOPER, HIGH, issue, recommendation)
            )
        else:
            issue = 'Your application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) read requests'.format(
                read_consecutive / total_reads * 100.0,
                read_sequential / total_reads * 100.0
            )

            insights_operation.append(
                message(TARGET_DEVELOPER, OK, issue, None)
            )

    write_consecutive = df['counters']['POSIX_CONSEC_WRITES'].sum()
    #print('WRITE Consecutive: {} ({:.2f}%)'.format(write_consecutive, write_consecutive / total_writes * 100))

    write_sequential = df['counters']['POSIX_SEQ_WRITES'].sum()
    write_sequential -= write_consecutive
    #print('WRITE Sequential: {} ({:.2f}%)'.format(write_sequential, write_sequential / total_writes * 100))

    write_random = total_writes - write_consecutive - write_sequential
    #print('WRITE Random: {} ({:.2f}%)'.format(write_random, write_random / total_writes * 100))

    if total_writes:
        if write_random and write_random / total_writes > THRESHOLD_RANDOM_OPERATIONS:
            issue = 'Application is issuing a high number ({}) of random write operations ({:.2f}%)'.format(
                write_random, write_random / total_writes * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider changing your data model to have consecutive or sequential writes'
                }
            ]

            insights_operation.append(
                message(TARGET_DEVELOPER, HIGH, issue, recommendation)
            )
        else:
            issue = 'Your application mostly uses consecutive ({:.2f}%) and sequential ({:.2f}%) write requests'.format(
                write_consecutive / total_writes * 100.0,
                write_sequential / total_writes * 100.0
            )

            insights_operation.append(
                message(TARGET_DEVELOPER, OK, issue, None)
            )

    #########################################################################################################################################################################

    # Shared file with small operations
    # print(df['counters'].loc[(df['counters']['rank'] == -1)])

    shared_files = df['counters'].loc[(df['counters']['rank'] == -1)]

    if not shared_files.empty:
        total_shared_reads = shared_files['POSIX_READS'].sum()
        total_shared_reads_small = shared_files['POSIX_SIZE_READ_0_100'].sum() + shared_files['POSIX_SIZE_READ_1K_10K'].sum() + shared_files['POSIX_SIZE_READ_100K_1M'].sum()

        if total_shared_reads_small / total_shared_reads > THRESHOLD_SMALL_REQUESTS:
            issue = 'Application issues a high number ({}) of small read requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file read requests'.format(
                total_shared_reads_small, total_shared_reads_small / total_shared_reads * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider coalesceing read requests into larger more contiguous ones using MPI-IO collective operations',
                    'sample': Syntax.from_path('snippets/mpi-io-collective-read.c', line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(TARGET_DEVELOPER, HIGH, issue, recommendation)
            )

        total_shared_writes = shared_files['POSIX_WRITES'].sum()
        total_shared_writes_small = shared_files['POSIX_SIZE_WRITE_0_100'].sum() + shared_files['POSIX_SIZE_WRITE_1K_10K'].sum() + shared_files['POSIX_SIZE_WRITE_100K_1M'].sum()

        if total_shared_writes_small / total_shared_writes > THRESHOLD_SMALL_REQUESTS:
            issue = 'Application issues a high number ({}) of small write requests to a shared file (i.e., < 1MB) which represents {:.2f}% of all shared file write requests'.format(
                total_shared_writes_small, total_shared_writes_small / total_shared_writes * 100.0
            )

            recommendation = [
                {
                    'message': 'Consider coalescing write requests into larger more contiguous ones using MPI-IO collective operations',
                    'sample': Syntax.from_path('snippets/mpi-io-collective-write.c', line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(TARGET_DEVELOPER, HIGH, issue, recommendation)
            )

    #########################################################################################################################################################################

    has_long_metadata = df['fcounters'][(df['fcounters']['POSIX_F_META_TIME'] > THRESHOLD_METADATA_TIME_RANK)]

    if not has_long_metadata.empty:
        issue = 'There are {} ranks where metadata operations take over {} seconds'.format(
            len(has_long_metadata), THRESHOLD_METADATA_TIME_RANK
        )

        recommendation = [
            {
                'message': 'Attempt to combine files, reduce, or cache metadata operations'
            }
        ]

        if 'HDF5' in modules:
            recommendation.append(
                {
                    'message': 'Since your appplication uses HDF5, try enabling collective metadata calls with H5Pset_coll_metadata_write() and H5Pset_all_coll_metadata_ops()',
                    'sample': Syntax.from_path('snippets/hdf5-collective-metadata.c', line_numbers=True, background_color='default')
                },
                {
                    'message': 'Since your appplication uses HDF5, try using metadata cache to defer metadata operations',
                    'sample': Syntax.from_path('snippets/hdf5-cache.c', line_numbers=True, background_color='default')
                }
            )

        insights_metadata.append(
            message(TARGET_DEVELOPER, HIGH, issue, recommendation)
        )

    # We already have a single line for each shared-file access
    # To check for stragglers, we can check the difference between the 

    # POSIX_FASTEST_RANK_BYTES
    # POSIX_SLOWEST_RANK_BYTES
    # POSIX_F_VARIANCE_RANK_BYTES

    stragglers_count = 0

    for index, row in shared_files.iterrows():
        total_transfer_size = row['POSIX_BYTES_WRITTEN'] + row['POSIX_BYTES_READ']

        if total_transfer_size and (row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size > THRESHOLD_STRAGGLERS:
            stragglers_count += 1

    if stragglers_count:
        issue = 'We detected data transfer imbalance caused by stragglers when accessing {} shared file.'.format(
            stragglers_count
        )

        recommendation = [
            {
                'message': 'Consider better balancing the data transfer between the application ranks'
            },
            {
                'message': 'Consider tuning how your data is distributed in the file system by changing the stripe size and count',
                'sample': Syntax.from_path('snippets/lustre-striping.bash', line_numbers=True, background_color='default')
            }
        ]

        insights_operation.append(
            message(TARGET_USER, HIGH, issue, recommendation)
        )

    # POSIX_F_FASTEST_RANK_TIME
    # POSIX_F_SLOWEST_RANK_TIME
    # POSIX_F_VARIANCE_RANK_TIME

    shared_files_times = df['fcounters'].loc[(df['fcounters']['rank'] == -1)]

    stragglers_count = 0
    stragglers_imbalance = {}

    for index, row in shared_files_times.iterrows():
        total_transfer_time = row['POSIX_F_MAX_WRITE_TIME'] + row['POSIX_F_MAX_READ_TIME']

        if total_transfer_time and (row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time > THRESHOLD_STRAGGLERS:
            stragglers_count += 1

    if stragglers_count:
        issue = 'We detected time imbalance caused by stragglers when accessing {} shared file.'.format(
            stragglers_count
        )

        recommendation = [
            {
                'message': 'Consider better distributing the data in the parallel file system' # needs to review what suggestion to give
            },
            {
                'message': 'Consider tuning how your data is distributed in the file system by changing the stripe size and count',
                'sample': Syntax.from_path('snippets/lustre-striping.bash', line_numbers=True, background_color='default')
            }
        ]

        insights_operation.append(
            message(TARGET_USER, HIGH, issue, recommendation)
        )


#########################################################################################################################################################################

if 'MPI-IO' in report.records:
    # Check if application uses MPI-IO and collective operations
    df_mpiio = report.records['MPI-IO'].to_df()

    #print(df_mpiio)

    df_mpiio_collective_reads = df_mpiio['counters'].loc[(df_mpiio['counters']['MPIIO_COLL_READS'] > 0)]

    total_mpiio_read_operations = df_mpiio['counters']['MPIIO_INDEP_READS'].sum() + df_mpiio['counters']['MPIIO_COLL_READS'].sum()

    if df_mpiio_collective_reads.empty:
        if total_mpiio_read_operations:
            issue = 'Your application uses MPI-IO but it does not use collective read operations, instead it issues {} ({:.2f}%) independent read calls'.format(
                df_mpiio['counters']['MPIIO_INDEP_READS'].sum(),
                df_mpiio['counters']['MPIIO_INDEP_READS'].sum() / (total_mpiio_read_operations) * 100
            )

            recommendation = [
                {
                    'message': 'Use collective read operations (e.g. MPI_File_read_all() or MPI_File_read_at_all()) and set one aggregator per compute node',
                    'sample': Syntax.from_path('snippets/mpi-io-collective-read.c', line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(TARGET_DEVELOPER, HIGH, issue, recommendation)
            )
    else:
        issue = 'Your application uses MPI-IO and read data using {} ({:.2f}%) collective operations'.format(
            df_mpiio['counters']['MPIIO_COLL_READS'].sum(),
            df_mpiio['counters']['MPIIO_COLL_READS'].sum() / (df_mpiio['counters']['MPIIO_INDEP_READS'].sum() + df_mpiio['counters']['MPIIO_COLL_READS'].sum()) * 100
        )

        insights_operation.append(
            message(TARGET_DEVELOPER, OK, issue)
        )

    df_mpiio_collective_writes = df_mpiio['counters'].loc[(df_mpiio['counters']['MPIIO_COLL_WRITES'] > 0)]

    total_mpiio_write_operations = df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() + df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()

    if df_mpiio_collective_writes.empty:
        if total_mpiio_write_operations:
            issue = 'Your application uses MPI-IO but it does not use collective write operations, instead it issues {} ({:.2f}%) independent write calls'.format(
                df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum(),
                df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() / (total_mpiio_write_operations) * 100
            )

            recommendation = [
                {
                    'message': 'Use collective write operations (e.g. MPI_File_write_all() or MPI_File_write_at_all()) and set one aggregator per compute node',
                    'sample': Syntax.from_path('snippets/mpi-io-collective-write.c', line_numbers=True, background_color='default')
                }
            ]

            insights_operation.append(
                message(TARGET_DEVELOPER, HIGH, issue, recommendation)
            )
    else:
        issue = 'Your application uses MPI-IO and write data using {} ({:.2f}%) collective operations'.format(
            df_mpiio['counters']['MPIIO_COLL_WRITES'].sum(),
            df_mpiio['counters']['MPIIO_COLL_WRITES'].sum() / (df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() + df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()) * 100
        )

        insights_operation.append(
            message(TARGET_DEVELOPER, OK, issue)
        )

    #########################################################################################################################################################################

    # Look for usage of non-block operations

    if df_mpiio['counters']['MPIIO_NB_READS'].sum() == 0:
        issue = 'Your application does not use non-blocking (asynchronous) reads'

        recommendation = [
            {
                'message': 'If you use HDF5, considering using the ASYNC VOL connector (https://github.com/hpc-io/vol-async)',
                'sample': Syntax.from_path('snippets/hdf5-vol-async.c', line_numbers=True, background_color='default')
            },
            {
                'message': 'If you use MPI-IO, consider non-blocking/asynchronous I/O operations (e.g., MPI_File_iread(), MPI_File_read_all_begin/end(), or MPI_File_read_at_all_begin/end())',
                'sample': Syntax.from_path('snippets/mpi-io-iread.c', line_numbers=True, background_color='default')
            }
        ]

        insights_operation.append(
            message(TARGET_DEVELOPER, WARN, issue, recommendation)
        )

    if df_mpiio['counters']['MPIIO_NB_WRITES'].sum() == 0:
        issue = 'Your application does not use non-blocking (asynchronous) writes'

        recommendation = [
            {
                'message': 'If you use HDF5, considering using the ASYNC VOL connector (https://github.com/hpc-io/vol-async)',
                'sample': Syntax.from_path('snippets/hdf5-vol-async.c', line_numbers=True, background_color='default')
            },
            {
                'message': 'If you use MPI-IO, consider non-blocking/asynchronous I/O operations (e.g., MPI_File_iwrite(), MPI_File_write_all_begin/end(), or MPI_File_write_at_all_begin/end())',
                'sample': Syntax.from_path('snippets/mpi-io-iwrite.c', line_numbers=True, background_color='default')
            }
        ]

        insights_operation.append(
            message(TARGET_DEVELOPER, WARN, issue, recommendation)
        )

#########################################################################################################################################################################

# Nodes and MPI-IO aggregators
# If the application uses collective reads or collective writes, look for the number of aggregators
metadata = job['metadata']

if 'h' in metadata:
    hints = metadata['h']

    if hints:
        hints = hints.split(';')

issue = 'Detected MPI-IO hints: {}'.format(' '.join(hints))

insights_operation.append(
    message(TARGET_USER, INFO, issue)
)

# print('Hints: ', hints)

#########################################################################################################################################################################

cb_nodes = None

for hint in hints:
    (key, value) = hint.split('=')
    
    if key == 'cb_nodes':
        cb_nodes = value

# Do we have one MPI-IO aggregator per node?
if cb_nodes != NUMBER_OF_COMPUTE_NODES:
    issue = 'Your application is using inter-node aggregators (which require network communication)'

    recommendation = [
        {
            'message': 'Set the MPI hints for the number of aggregators as one per compute node (e.g., cb_nodes={})'.format(
                NUMBER_OF_COMPUTE_NODES
            ),
            'sample': Syntax.from_path('snippets/mpi-io-hints.bash', line_numbers=True, background_color='default')
        }
    ]

    insights_operation.append(
        message(TARGET_USER, WARN, issue, recommendation)
    )

#########################################################################################################################################################################

insights_end_time = time.time()

job_start = datetime.datetime.fromtimestamp(job['start_time'], datetime.timezone.utc)
job_end = datetime.datetime.fromtimestamp(job['end_time'], datetime.timezone.utc)

console.print()

console.print(
    Panel(
        '\n'.join([
            ' [b]JOB[/b]:       [white]{}[/white]'.format(job['jobid']),
            ' [b]DARSHAN[/b]:   [white]{}[/white]'.format(filename),
            ' [b]DATE[/b]:      [white]{} to {} ({:.2f} hours)[/white]'.format(
                job_start,
                job_end,
                (job_end - job_start).total_seconds() / 3600
            )
        ]),
        title='[b]I/O INSIGHTS v.0.2[/b]',
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
            Group(
                *insights_metadata
            ),
            title='METADATA',
            title_align='left',
            padding=1
        )
    )

if insights_operation:
    console.print(
        Panel(
            Group(
                *insights_operation
            ),
            title='OPERATIONS',
            title_align='left',
            padding=1
        )
    )

console.print(
    Panel(
        ' {} | [white]LBL[/white] | [white]I/O Insights report generated at {} in[/white] {:.3f} seconds'.format(
            datetime.datetime.now().year,
            datetime.datetime.now(),
            insights_end_time - insights_start_time
        ),
        box=box.SIMPLE
    )
)

if args.export_html:
    console.save_html(
        'io-insights.html'
    )

