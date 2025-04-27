#!/usr/bin/env python3

import csv
import datetime
import io
import os
import shlex
import shutil
import subprocess
import sys
import time

import darshan  # type: ignore
import darshan.backend.cffi_backend as darshanll  # type: ignore
import numpy as np
import pandas as pd
from packaging import version
from rich import print
from rich.padding import Padding
from rich.panel import Panel

from drishti.handlers.darshan_util import DarshanFile, ModuleType

from drishti.includes.config import (
    HIGH,
    RECOMMENDATIONS,
    WARN,
    init_console,
    insights_total,
    thresholds,
)

# from drishti.includes.module import *
import drishti.includes.module as module

# from drishti.includes.module import (
#     check_individual_read_imbalance,
#     check_individual_write_imbalance,
#     check_long_metadata,
#     check_misaligned,
#     check_mpi_aggregator,
#     check_mpi_collective_read_operation,
#     check_mpi_collective_write_operation,
#     check_mpi_none_block_operation,
#     check_mpiio,
#     check_operation_intensive,
#     check_random_operation,
#     check_shared_data_imblance,
#     check_shared_small_operation,
#     check_shared_time_imbalance,
#     check_size_intensive,
#     check_small_operation,
#     check_stdio,
#     check_traffic,
#     display_content,
#     display_footer,
#     display_thresholds,
#     export_csv,
#     export_html,
#     export_svg,
# )
import drishti.includes.parser as parser
# from drishti.includes.parser import args


def is_available(name):
    """Check whether `name` is on PATH and marked as executable."""

    return shutil.which(name) is not None


def check_log_version(console, file, log_version, library_version):
    use_file = file

    if version.parse(log_version) < version.parse('3.4.0'):
        # Check if darshan-convert is installed and available in the PATH
        if not is_available('darshan-convert'):
            console.print(
                Panel(
                    Padding(
                        'Darshan file is using an old format and darshan-convert is not available in the PATH.',
                        (1, 1)
                    ),
                    title='{}WARNING'.format('[orange1]'),
                    title_align='left'
                )
            )

            sys.exit(os.EX_DATAERR)

        use_file = os.path.basename(file.replace('.darshan', '.converted.darshan'))

        console.print(
            Panel(
                Padding(
                    'Converting .darshan log from {}: format: saving output file "{}" in the current working directory.'.format(
                        log_version,
                        use_file
                    ),
                    (1, 1)
                ),
                title='{}WARNING'.format('[orange1]'),
                title_align='left'
            )
        )

        if not os.path.isfile(use_file):
            ret = os.system(
                'darshan-convert {} {}'.format(
                    file,
                    use_file
                )
            )

            if ret != 0:
                print('Unable to convert .darshan file to version {}'.format(library_version))

    return use_file


def handler():
    console = init_console()

    insights_start_time = time.time()

    darshan_log_path = parser.args.log_paths[0]
    log = darshanll.log_open(darshan_log_path)

    modules = darshanll.log_get_modules(log)

    information = darshanll.log_get_job(log)

    if 'log_ver' in information:
        log_version = information['log_ver']
    else:
        log_version = information['metadata']['lib_ver']  
    library_version = darshanll.get_lib_version()

    # Make sure log format is of the same version
    filename = darshan_log_path
    # check_log_version(console, darshan_log_path, log_version, library_version)
 
    darshanll.log_close(log)

    darshan.enable_experimental()

    report = darshan.DarshanReport(filename)

    job = report.metadata

    #########################################################################################################################################################################
    darshan_file_obj = DarshanFile(file_path=darshan_log_path)

    #########################################################################################################################################################################

    # Check usage of STDIO, POSIX, and MPI-IO per file

    if 'STDIO' in report.records:
        df_stdio = report.records['STDIO'].to_df()

        if df_stdio:
            total_write_size_stdio = df_stdio['counters']['STDIO_BYTES_WRITTEN'].sum()
            total_read_size_stdio = df_stdio['counters']['STDIO_BYTES_READ'].sum()

            total_size_stdio = total_write_size_stdio + total_read_size_stdio 
        else:
            total_size_stdio = 0
    else:
        df_stdio = None

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
        df_posix = None

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
        df_mpiio = None

        total_size_mpiio = 0
    
    dxt_posix = None
    dxt_posix_read_data = None
    dxt_posix_write_data = None
    dxt_mpiio = None

    df_lustre = None
    if "LUSTRE" in report.records:
        df_lustre = report.records['LUSTRE'].to_df()
    if parser.args.backtrace:
        if "DXT_POSIX" in report.records:
            dxt_posix = report.records["DXT_POSIX"].to_df()
            dxt_posix = pd.DataFrame(dxt_posix)
            if "address_line_mapping" not in dxt_posix:
                parser.args.backtrace = False
            else:
                read_id = []
                read_rank = []
                read_length = []
                read_offsets = []
                read_end_time = []
                read_start_time = []
                read_operation = []

                write_id = []
                write_rank = []
                write_length = []
                write_offsets = []
                write_end_time = []
                write_start_time = []
                write_operation = []
                
                for r in zip(dxt_posix['rank'], dxt_posix['read_segments'], dxt_posix['write_segments'], dxt_posix['id']):
                    if not r[1].empty:
                        read_id.append([r[3]] * len((r[1]['length'].to_list())))
                        read_rank.append([r[0]] * len((r[1]['length'].to_list())))
                        read_length.append(r[1]['length'].to_list())
                        read_end_time.append(r[1]['end_time'].to_list())
                        read_start_time.append(r[1]['start_time'].to_list())
                        read_operation.append(['read'] * len((r[1]['length'].to_list())))
                        read_offsets.append(r[1]['offset'].to_list())

                    if not r[2].empty:
                        write_id.append([r[3]] * len((r[2]['length'].to_list())))     
                        write_rank.append([r[0]] * len((r[2]['length'].to_list())))
                        write_length.append(r[2]['length'].to_list())
                        write_end_time.append(r[2]['end_time'].to_list())
                        write_start_time.append(r[2]['start_time'].to_list())
                        write_operation.append(['write'] * len((r[2]['length'].to_list())))
                        write_offsets.append(r[2]['offset'].to_list())

                read_id = [element for nestedlist in read_id for element in nestedlist]
                read_rank = [element for nestedlist in read_rank for element in nestedlist]
                read_length = [element for nestedlist in read_length for element in nestedlist]
                read_offsets = [element for nestedlist in read_offsets for element in nestedlist]
                read_end_time = [element for nestedlist in read_end_time for element in nestedlist]
                read_operation = [element for nestedlist in read_operation for element in nestedlist]
                read_start_time = [element for nestedlist in read_start_time for element in nestedlist]
                
                write_id = [element for nestedlist in write_id for element in nestedlist]
                write_rank = [element for nestedlist in write_rank for element in nestedlist]
                write_length = [element for nestedlist in write_length for element in nestedlist]
                write_offsets = [element for nestedlist in write_offsets for element in nestedlist]
                write_end_time = [element for nestedlist in write_end_time for element in nestedlist]
                write_operation = [element for nestedlist in write_operation for element in nestedlist]
                write_start_time = [element for nestedlist in write_start_time for element in nestedlist]

                dxt_posix_read_data = pd.DataFrame(
                    {
                    'id': read_id,
                    'rank': read_rank,
                    'length': read_length,
                    'end_time': read_end_time,
                    'start_time': read_start_time,
                    'operation': read_operation,
                    'offsets': read_offsets,
                    })

                dxt_posix_write_data = pd.DataFrame(
                    {
                    'id': write_id,
                    'rank': write_rank,
                    'length': write_length,
                    'end_time': write_end_time,
                    'start_time': write_start_time,
                    'operation': write_operation,
                    'offsets': write_offsets,
                    })

            if "DXT_MPIIO" in report.records:
                dxt_mpiio = report.records["DXT_MPIIO"].to_df()
                dxt_mpiio = pd.DataFrame(dxt_mpiio)
            

    # Since POSIX will capture both POSIX-only accesses and those comming from MPI-IO, we can subtract those
    if total_size_posix > 0 and total_size_posix >= total_size_mpiio:
        total_size_posix -= total_size_mpiio

    total_size = total_size_stdio + total_size_posix + total_size_mpiio

    assert(total_size_stdio >= 0)
    assert(total_size_posix >= 0)
    assert(total_size_mpiio >= 0)

    files = {}

    # Check interface usage for each file
    file_map = report.name_records

    total_files = len(file_map)

    total_files_stdio = 0
    total_files_posix = 0
    total_files_mpiio = 0

    for id, path in file_map.items():
        if df_stdio:
            uses_stdio = len(df_stdio['counters'][(df_stdio['counters']['id'] == id)]) > 0
        else:
            uses_stdio = 0
        
        if df_posix:
            uses_posix = len(df_posix['counters'][(df_posix['counters']['id'] == id)]) > 0
        else:
            uses_posix = 0

        if df_mpiio:
            uses_mpiio = len(df_mpiio['counters'][(df_mpiio['counters']['id'] == id)]) > 0
        else:
            uses_mpiio = 0

        total_files_stdio += uses_stdio
        total_files_posix += uses_posix
        total_files_mpiio += uses_mpiio

        files[id] = {
            'path': path,
            'stdio': uses_stdio,
            'posix': uses_posix,
            'mpiio': uses_mpiio
        }

    # module.check_stdio(total_size, total_size_stdio)
    module.check_stdio(total_size=darshan_file_obj.io_stats.total_bytes, total_size_stdio=darshan_file_obj.io_stats.stdio_size)
    # module.check_mpiio(modules)
    module.check_mpiio(modules=darshan_file_obj.modules)

    #########################################################################################################################################################################

    if 'POSIX' in report.records:
        df = report.records['POSIX'].to_df()

        #########################################################################################################################################################################

        # Get number of write/read operations
        total_reads = df['counters']['POSIX_READS'].sum()
        total_writes = df['counters']['POSIX_WRITES'].sum()

        # Get total number of I/O operations
        total_operations = total_writes + total_reads

        # To check whether the application is write-intensive or read-intensive we only look at the POSIX level and check if the difference between reads and writes is larger than 10% (for more or less), otherwise we assume a balance
        # module.check_operation_intensive(total_operations, total_reads, total_writes)
        module.check_operation_intensive(
            total_operations=darshan_file_obj.io_stats.posix_ops,
            total_reads=darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX, "read"),
            total_writes=darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX, "write"),
        )

        total_read_size = df['counters']['POSIX_BYTES_READ'].sum()
        total_written_size = df['counters']['POSIX_BYTES_WRITTEN'].sum()

        total_size = total_written_size + total_read_size

        # module.check_size_intensive(total_size, total_read_size, total_written_size)
        module.check_size_intensive(
            total_size=darshan_file_obj.io_stats.total_bytes,
            total_read_size=darshan_file_obj.io_stats.get_module_size(ModuleType.POSIX, "read"),
            total_written_size=darshan_file_obj.io_stats.get_module_size(ModuleType.POSIX, "write"),
        )

        #########################################################################################################################################################################

        # Get the number of small I/O operations (less than 1 MB)
        total_reads_small = (
            df['counters']['POSIX_SIZE_READ_0_100'].sum() +
            df['counters']['POSIX_SIZE_READ_100_1K'].sum() +
            df['counters']['POSIX_SIZE_READ_1K_10K'].sum() +
            df['counters']['POSIX_SIZE_READ_10K_100K'].sum() +
            df['counters']['POSIX_SIZE_READ_100K_1M'].sum()
        )

        total_writes_small = (
            df['counters']['POSIX_SIZE_WRITE_0_100'].sum() +
            df['counters']['POSIX_SIZE_WRITE_100_1K'].sum() +
            df['counters']['POSIX_SIZE_WRITE_1K_10K'].sum() +
            df['counters']['POSIX_SIZE_WRITE_10K_100K'].sum() +
            df['counters']['POSIX_SIZE_WRITE_100K_1M'].sum()
        )

        # Get the files responsible for more than half of these accesses
        files = []

        df['counters']['INSIGHTS_POSIX_SMALL_READ'] = (
            df['counters']['POSIX_SIZE_READ_0_100'] +
            df['counters']['POSIX_SIZE_READ_100_1K'] +
            df['counters']['POSIX_SIZE_READ_1K_10K'] +
            df['counters']['POSIX_SIZE_READ_10K_100K'] +
            df['counters']['POSIX_SIZE_READ_100K_1M']
        )

        df['counters']['INSIGHTS_POSIX_SMALL_WRITE'] = (
            df['counters']['POSIX_SIZE_WRITE_0_100'] +
            df['counters']['POSIX_SIZE_WRITE_100_1K'] +
            df['counters']['POSIX_SIZE_WRITE_1K_10K'] +
            df['counters']['POSIX_SIZE_WRITE_10K_100K'] +
            df['counters']['POSIX_SIZE_WRITE_100K_1M']
        )

        detected_files = pd.DataFrame(df['counters'].groupby('id')[['INSIGHTS_POSIX_SMALL_READ', 'INSIGHTS_POSIX_SMALL_WRITE']].sum()).reset_index()
        detected_files.columns = ['id', 'total_reads', 'total_writes']
        detected_files.loc[:, 'id'] = detected_files.loc[:, 'id'].astype(str)


        # module.check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules, file_map, dxt_posix, dxt_posix_read_data, dxt_posix_write_data)
        module.check_small_operation(
            total_reads=darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX, "read"),
            total_reads_small=darshan_file_obj.posix_small_io.read,
            total_writes=darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX, "write"),
            total_writes_small=darshan_file_obj.posix_small_io.write,
            detected_files=darshan_file_obj.posix_detected_small_files, modules=darshan_file_obj.modules,
            file_map=darshan_file_obj.file_map,
            dxt_posix=darshan_file_obj.dxt_posix_df,
            dxt_posix_read_data=darshan_file_obj.dxt_posix_read_df,
            dxt_posix_write_data=darshan_file_obj.dxt_posix_write_df,
        )

        #########################################################################################################################################################################

        # How many requests are misaligned?

        total_mem_not_aligned = df['counters']['POSIX_MEM_NOT_ALIGNED'].sum()
        total_file_not_aligned = df['counters']['POSIX_FILE_NOT_ALIGNED'].sum()

        # module.check_misaligned(total_operations, total_mem_not_aligned, total_file_not_aligned, modules, file_map, df_lustre, dxt_posix, dxt_posix_read_data)
        module.check_misaligned(
            total_operations=darshan_file_obj.io_stats.posix_ops,
            total_mem_not_aligned=darshan_file_obj.mem_not_aligned,
            total_file_not_aligned=darshan_file_obj.file_not_aligned,
            modules=darshan_file_obj.modules,
            file_map=darshan_file_obj.file_map,
            df_lustre=darshan_file_obj.lustre_df,
            dxt_posix=darshan_file_obj.dxt_posix_df,
            dxt_posix_read_data=darshan_file_obj.dxt_posix_read_df,
        )

        #########################################################################################################################################################################

        # Redundant read-traffic (based on Phill)
        # POSIX_MAX_BYTE_READ (Highest offset in the file that was read)
        max_read_offset = df['counters']['POSIX_MAX_BYTE_READ'].max()
        max_write_offset = df['counters']['POSIX_MAX_BYTE_WRITTEN'].max()

        # module.check_traffic(max_read_offset, total_read_size, max_write_offset, total_written_size, dxt_posix, dxt_posix_read_data, dxt_posix_write_data)
        module.check_traffic(
            max_read_offset=darshan_file_obj.max_read_offset,
            total_read_size=darshan_file_obj.io_stats.get_module_size(ModuleType.POSIX, "read"),
            max_write_offset=darshan_file_obj.max_write_offset,
            total_written_size=darshan_file_obj.io_stats.get_module_size(ModuleType.POSIX, "write"),
            dxt_posix=darshan_file_obj.dxt_posix_df,
            dxt_posix_read_data=darshan_file_obj.dxt_posix_read_df,
            dxt_posix_write_data=darshan_file_obj.dxt_posix_write_df,
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


        write_consecutive = df['counters']['POSIX_CONSEC_WRITES'].sum()

        write_sequential = df['counters']['POSIX_SEQ_WRITES'].sum()
        write_sequential -= write_consecutive

        write_random = total_writes - write_consecutive - write_sequential
        #print('WRITE Random: {} ({:.2f}%)'.format(write_random, write_random / total_writes * 100))


        assert read_consecutive == darshan_file_obj.posix_read_consecutive
        assert read_sequential == darshan_file_obj.posix_read_sequential
        assert read_random == darshan_file_obj.posix_read_random, f"{read_random} != {darshan_file_obj.posix_read_random}"
        assert total_reads == darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX,"read"), f"{total_reads} != {darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX, "read")}"
        assert write_consecutive == darshan_file_obj.posix_write_consecutive
        assert write_sequential == darshan_file_obj.posix_write_sequential
        assert write_random == darshan_file_obj.posix_write_random
        assert total_writes == darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX,"write")

        # module.check_random_operation(read_consecutive, read_sequential, read_random, total_reads, write_consecutive, write_sequential, write_random, total_writes, dxt_posix, dxt_posix_read_data, dxt_posix_write_data)
        module.check_random_operation(
            read_consecutive=darshan_file_obj.posix_read_consecutive,
            read_sequential=darshan_file_obj.posix_read_sequential,
            read_random=darshan_file_obj.posix_read_random,
            total_reads=darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX,"read"),
            write_consecutive=darshan_file_obj.posix_write_consecutive,
            write_sequential=darshan_file_obj.posix_write_sequential,
            write_random=darshan_file_obj.posix_write_random,
            total_writes=darshan_file_obj.io_stats.get_module_ops(ModuleType.POSIX,"write"),
            dxt_posix=darshan_file_obj.dxt_posix_df,
            dxt_posix_read_data=darshan_file_obj.dxt_posix_read_df,
            dxt_posix_write_data=darshan_file_obj.dxt_posix_write_df,
        )

        #########################################################################################################################################################################

        # Shared file with small operations

        shared_files = df['counters'].loc[(df['counters']['rank'] == -1)]

        shared_files = shared_files.assign(id=lambda d: d['id'].astype(str))

        if not shared_files.empty:
            # TODO: This entire conditional
            total_shared_reads = shared_files['POSIX_READS'].sum()
            total_shared_reads_small = (
                shared_files['POSIX_SIZE_READ_0_100'].sum() +
                shared_files['POSIX_SIZE_READ_100_1K'].sum() +
                shared_files['POSIX_SIZE_READ_1K_10K'].sum() +
                shared_files['POSIX_SIZE_READ_10K_100K'].sum() +
                shared_files['POSIX_SIZE_READ_100K_1M'].sum()
            )

            shared_files['INSIGHTS_POSIX_SMALL_READS'] = (
                shared_files['POSIX_SIZE_READ_0_100'] +
                shared_files['POSIX_SIZE_READ_100_1K'] +
                shared_files['POSIX_SIZE_READ_1K_10K'] +
                shared_files['POSIX_SIZE_READ_10K_100K'] +
                shared_files['POSIX_SIZE_READ_100K_1M']
            )


            total_shared_writes = shared_files['POSIX_WRITES'].sum()
            total_shared_writes_small = (
                shared_files['POSIX_SIZE_WRITE_0_100'].sum() +
                shared_files['POSIX_SIZE_WRITE_100_1K'].sum() +
                shared_files['POSIX_SIZE_WRITE_1K_10K'].sum() +
                shared_files['POSIX_SIZE_WRITE_10K_100K'].sum() +
                shared_files['POSIX_SIZE_WRITE_100K_1M'].sum()
            )

            shared_files['INSIGHTS_POSIX_SMALL_WRITES'] = (
                shared_files['POSIX_SIZE_WRITE_0_100'] +
                shared_files['POSIX_SIZE_WRITE_100_1K'] +
                shared_files['POSIX_SIZE_WRITE_1K_10K'] +
                shared_files['POSIX_SIZE_WRITE_10K_100K'] +
                shared_files['POSIX_SIZE_WRITE_100K_1M']
            )

            # module.check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes, total_shared_writes_small, shared_files, file_map)
            assert total_shared_reads == darshan_file_obj.posix_shared_reads
            sys.exit(2)
            module.check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes, total_shared_writes_small, shared_files, file_map)

        #########################################################################################################################################################################

        count_long_metadata = len(df['fcounters'][(df['fcounters']['POSIX_F_META_TIME'] > thresholds['metadata_time_rank'][0])])

        assert darshan_file_obj.posix_long_metadata_count == count_long_metadata
        assert darshan_file_obj.modules == modules.keys(), f"{darshan_file_obj.modules} != {modules.keys()}"
        # module.check_long_metadata(count_long_metadata, modules)
        module.check_long_metadata(count_long_metadata=darshan_file_obj.posix_long_metadata_count, modules=darshan_file_obj.modules)

        # We already have a single line for each shared-file access
        # To check for stragglers, we can check the difference between the

        # POSIX_FASTEST_RANK_BYTES
        # POSIX_SLOWEST_RANK_BYTES
        # POSIX_F_VARIANCE_RANK_BYTES

        stragglers_count = 0

        shared_files = shared_files.assign(id=lambda d: d['id'].astype(str))

        # Get the files responsible
        detected_files = []

        for index, row in shared_files.iterrows():
            total_transfer_size = row['POSIX_BYTES_WRITTEN'] + row['POSIX_BYTES_READ']

            if total_transfer_size and abs(row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size > thresholds['imbalance_stragglers'][0]:
                stragglers_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size * 100
                ])

        column_names = ['id', 'data_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        assert stragglers_count == darshan_file_obj.posix_data_stragglers_count, f"{stragglers_count} != {darshan_file_obj.posix_data_stragglers_count}"
        assert detected_files.equals(darshan_file_obj.posix_data_stragglers_df), f"{detected_files} != {darshan_file_obj.posix_data_stragglers_df}"
        assert file_map == darshan_file_obj.file_map, f"{file_map} != {darshan_file_obj.file_map}"
        assert dxt_posix == darshan_file_obj.dxt_posix_df, f"{dxt_posix} != {darshan_file_obj.dxt_posix_df}"
        assert dxt_posix_read_data == darshan_file_obj.dxt_posix_read_df, f"{dxt_posix_read_data} != {darshan_file_obj.dxt_posix_read_df}"
        assert dxt_posix_write_data == darshan_file_obj.dxt_posix_write_df, f"{dxt_posix_write_data} != {darshan_file_obj.dxt_posix_write_df}"
        # module.check_shared_data_imblance(stragglers_count, detected_files, file_map, dxt_posix, dxt_posix_read_data, dxt_posix_write_data)
        module.check_shared_data_imblance(
            stragglers_count=darshan_file_obj.posix_data_stragglers_count,
            detected_files=darshan_file_obj.posix_data_stragglers_df,
            file_map=darshan_file_obj.file_map,
            dxt_posix=darshan_file_obj.dxt_posix_df,
            dxt_posix_read_data = darshan_file_obj.dxt_posix_read_df,
            dxt_posix_write_data = darshan_file_obj.dxt_posix_write_df
        )

        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME

        shared_files_times = df['fcounters'].loc[(df['fcounters']['rank'] == -1)]

        # Get the files responsible
        detected_files = []

        stragglers_count = 0
        # stragglers_imbalance = {}

        shared_files_times = shared_files_times.assign(id=lambda d: d['id'].astype(str))

        for index, row in shared_files_times.iterrows():
            total_transfer_time = row['POSIX_F_WRITE_TIME'] + row['POSIX_F_READ_TIME'] + row['POSIX_F_META_TIME']

            if total_transfer_time and abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time > thresholds['imbalance_stragglers'][0]:
                stragglers_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time * 100
                ])

        column_names = ['id', 'time_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        assert stragglers_count == darshan_file_obj.posix_time_stragglers_count, f"{stragglers_count} != {darshan_file_obj.posix_time_stragglers_count}"
        assert detected_files.equals(darshan_file_obj.posix_time_stragglers_df), f"{detected_files} != {darshan_file_obj.posix_time_stragglers_df}"
        assert file_map == darshan_file_obj.file_map, f"{file_map} != {darshan_file_obj.file_map}"

        # module.check_shared_time_imbalance(stragglers_count, detected_files, file_map)
        module.check_shared_time_imbalance(
            stragglers_count=darshan_file_obj.posix_time_stragglers_count,
            detected_files=darshan_file_obj.posix_time_stragglers_df,
            file_map=darshan_file_obj.file_map,
        )
        sys.exit(2)

        aggregated = df['counters'].loc[(df['counters']['rank'] != -1)][
            ['rank', 'id', 'POSIX_BYTES_WRITTEN', 'POSIX_BYTES_READ']
        ].groupby('id', as_index=False).agg({
            'rank': 'nunique',
            'POSIX_BYTES_WRITTEN': ['sum', 'min', 'max'],
            'POSIX_BYTES_READ': ['sum', 'min', 'max']
        })

        aggregated.columns = list(map('_'.join, aggregated.columns.values))

        aggregated = aggregated.assign(id=lambda d: d['id_'].astype(str))

        # Get the files responsible
        imbalance_count = 0

        detected_files = []

        for index, row in aggregated.iterrows():
            if row['POSIX_BYTES_WRITTEN_max'] and abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row['POSIX_BYTES_WRITTEN_max'] > thresholds['imbalance_size'][0]:
                imbalance_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row['POSIX_BYTES_WRITTEN_max'] * 100
                ])

        column_names = ['id', 'write_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        module.check_individual_write_imbalance(imbalance_count, detected_files, file_map, dxt_posix, dxt_posix_write_data)

        imbalance_count = 0

        detected_files = []

        for index, row in aggregated.iterrows():
            if row['POSIX_BYTES_READ_max'] and abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] > thresholds['imbalance_size'][0]:
                imbalance_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] * 100
                ])

        column_names = ['id', 'read_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        module.check_individual_read_imbalance(imbalance_count, detected_files, file_map, dxt_posix, dxt_posix_read_data)

    #########################################################################################################################################################################

    if 'MPI-IO' in report.records:
        # Check if application uses MPI-IO and collective operations
        df_mpiio = report.records['MPI-IO'].to_df()

        df_mpiio['counters'] = df_mpiio['counters'].assign(id=lambda d: d['id'].astype(str))

        # Get the files responsible
        detected_files = []

        df_mpiio_collective_reads = df_mpiio['counters']  #.loc[(df_mpiio['counters']['MPIIO_COLL_READS'] > 0)]

        total_mpiio_read_operations = df_mpiio['counters']['MPIIO_INDEP_READS'].sum() + df_mpiio['counters']['MPIIO_COLL_READS'].sum()

        mpiio_coll_reads = df_mpiio['counters']['MPIIO_COLL_READS'].sum()
        mpiio_indep_reads = df_mpiio['counters']['MPIIO_INDEP_READS'].sum()

        detected_files = []
        if mpiio_coll_reads == 0 and total_mpiio_read_operations and total_mpiio_read_operations > thresholds['collective_operations_absolute'][0]:
            files = pd.DataFrame(df_mpiio_collective_reads.groupby('id').sum()).reset_index()
            for index, row in df_mpiio_collective_reads.iterrows():
                if ((row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and
                    row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > thresholds['collective_operations'][0] and
                    (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > thresholds['collective_operations_absolute'][0]):

                    detected_files.append([
                        row['id'], row['MPIIO_INDEP_READS'], row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100
                    ])
        
        column_names = ['id', 'absolute_indep_reads', 'percent_indep_reads']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        module.check_mpi_collective_read_operation(mpiio_coll_reads, mpiio_indep_reads, total_mpiio_read_operations, detected_files, file_map, dxt_mpiio)

        df_mpiio_collective_writes = df_mpiio['counters']  #.loc[(df_mpiio['counters']['MPIIO_COLL_WRITES'] > 0)]

        total_mpiio_write_operations = df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() + df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()

        mpiio_coll_writes = df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()
        mpiio_indep_writes = df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum()

        detected_files = []
        if mpiio_coll_writes == 0 and total_mpiio_write_operations and total_mpiio_write_operations > thresholds['collective_operations_absolute'][0]:
            files = pd.DataFrame(df_mpiio_collective_writes.groupby('id').sum()).reset_index()

            for index, row in df_mpiio_collective_writes.iterrows():
                if ((row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and 
                    row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > thresholds['collective_operations'][0] and 
                    (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) > thresholds['collective_operations_absolute'][0]):

                    detected_files.append([
                        row['id'], row['MPIIO_INDEP_WRITES'], row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100
                    ])

        column_names = ['id', 'absolute_indep_writes', 'percent_indep_writes']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        module.check_mpi_collective_write_operation(mpiio_coll_writes, mpiio_indep_writes, total_mpiio_write_operations, detected_files, file_map, dxt_mpiio)

        #########################################################################################################################################################################

        # Look for usage of non-block operations

        # Look for HDF5 file extension

        has_hdf5_extension = False

        for index, row in df_mpiio['counters'].iterrows():
            if file_map[int(row['id'])].endswith('.h5') or file_map[int(row['id'])].endswith('.hdf5'):
                has_hdf5_extension = True

        mpiio_nb_reads = df_mpiio['counters']['MPIIO_NB_READS'].sum()
        mpiio_nb_writes = df_mpiio['counters']['MPIIO_NB_WRITES'].sum()

        module.check_mpi_none_block_operation(mpiio_nb_reads, mpiio_nb_writes, has_hdf5_extension, modules)

    #########################################################################################################################################################################

    # Nodes and MPI-IO aggregators
    # If the application uses collective reads or collective writes, look for the number of aggregators
    hints = ''

    if 'h' in job['job']['metadata']:
        hints = job['job']['metadata']['h']

        if hints:
            hints = hints.split(';')

    # print('Hints: ', hints)

    NUMBER_OF_COMPUTE_NODES = 0

    if 'MPI-IO' in modules:
        cb_nodes = None

        for hint in hints:
            if hint != 'no':
                (key, value) = hint.split('=')
            
            if key == 'cb_nodes':
                cb_nodes = value

        # Try to get the number of compute nodes from SLURM, if not found, set as information
        command = 'sacct --job {} --format=JobID,JobIDRaw,NNodes,NCPUs --parsable2 --delimiter ","'.format(
            job['job']['jobid']
        )

        arguments = shlex.split(command)

        try:
            result = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if result.returncode == 0:
                # We have successfully fetched the information from SLURM
                db = csv.DictReader(io.StringIO(result.stdout.decode('utf-8')))

                try:
                    first = next(db)

                    if 'NNodes' in first:
                        NUMBER_OF_COMPUTE_NODES = first['NNodes']

                        # Do we have one MPI-IO aggregator per node?
                        module.check_mpi_aggregator(cb_nodes, NUMBER_OF_COMPUTE_NODES)
                except StopIteration:
                    pass
        except FileNotFoundError:
            pass
    
    #########################################################################################################################################################################

    insights_end_time = time.time()

    # Version 3.4.1 of py-darshan changed the contents on what is reported in 'job'
    if 'start_time' in job['job']:
        job_start = datetime.datetime.fromtimestamp(job['job']['start_time'], datetime.timezone.utc)
        job_end = datetime.datetime.fromtimestamp(job['job']['end_time'], datetime.timezone.utc)
    else:
        job_start = datetime.datetime.fromtimestamp(job['job']['start_time_sec'], datetime.timezone.utc)
        job_end = datetime.datetime.fromtimestamp(job['job']['end_time_sec'], datetime.timezone.utc)

    console.print()

    console.print(
        Panel(
            '\n'.join([
                ' [b]JOB[/b]:            [white]{}[/white]'.format(
                    job['job']['jobid']
                ),
                ' [b]EXECUTABLE[/b]:     [white]{}[/white]'.format(
                    job['exe'].split()[0]
                ),
                ' [b]DARSHAN[/b]:        [white]{}[/white]'.format(
                    os.path.basename(darshan_log_path)
                ),
                ' [b]EXECUTION TIME[/b]: [white]{} to {} ({:.2f} hours)[/white]'.format(
                    job_start,
                    job_end,
                    (job_end - job_start).total_seconds() / 3600
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
                    job['job']['nprocs']
                ),
                ' [b]HINTS[/b]:          [white]{}[/white]'.format(
                    ' '.join(hints)
                )
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

    module.display_content(console)
    module.display_thresholds(console)
    module.display_footer(console, insights_start_time, insights_end_time)

    # Export to HTML, SVG, and CSV
    trace_name = os.path.basename(darshan_log_path).replace('.darshan', '')
    out_dir = parser.args.export_dir if parser.args.export_dir != "" else os.getcwd()

    module.export_html(console, out_dir, trace_name)
    module.export_svg(console, out_dir, trace_name)
    module.export_csv(out_dir, trace_name, job['job']['jobid'])
