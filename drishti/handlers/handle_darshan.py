#!/usr/bin/env python3
import collections
import dataclasses
from dataclasses import dataclass
import datetime
import io
import sys
import time
import shlex
import shutil
import subprocess
import typing

import pandas as pd
import darshan
import darshan.backend.cffi_backend as darshanll

from rich import print
from packaging import version
from includes.module import *
import includes.module as module
from includes.parser import args

from pprint import pprint


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


@dataclass
class TimestampPair:
    start: datetime.date
    end: datetime.date


@dataclass
class DarshanTrace:
    # Trace metadata
    path: str
    jobid: str
    log_ver: str
    time: TimestampPair
    exe: str

    # Report
    report: darshan.DarshanReport
    modules: typing.Iterable[str]

    stdio_df: pd.DataFrame = None
    posix_df: pd.DataFrame = None
    mpiio_df: pd.DataFrame = None
    lustre_df: pd.DataFrame = None

    dxt_posix: pd.DataFrame = None
    dxt_mpiio: pd.DataFrame = None

    dxt_posix_read_data: pd.DataFrame = None
    dxt_posix_write_data: pd.DataFrame = None

    total_write_size_stdio: int
    total_write_size_stdio: int
    total_size_stdio: int

    total_write_size_posix: int
    total_read_size_posix: int
    total_size_posix: int

    total_write_size_mpiio: int
    total_read_size_mpiio: int
    total_size_mpiio: int

    total_size: int
    total_files: int

    total_files_stdio: int = 0
    total_files_posix: int = 0
    total_files_mpiio: int = 0

    files: dict[str, dict[str, int]] = dataclasses.field(default_factory=dict)

    total_reads: int = 0
    total_writes: int = 0
    total_operations: int = 0
    total_read_size: int = 0
    total_written_size: int = 0
    total_size: int = 0
    total_reads_small: int = 0
    total_writes_small: int = 0

    def __init__(self, trace_path: str, job_information, report: darshan.DarshanReport):
        self.path = trace_path

        self.jobid = job_information['jobid']
        self.log_ver = job_information['log_ver'] if 'log_ver' in job_information else job_information['metadata'][
            'lib_ver']
        self.exe = report.metadata['exe']

        _start_time = datetime.datetime.fromtimestamp(job_information['start_time_sec'], tz=datetime.timezone.utc)
        _end_time = datetime.datetime.fromtimestamp(job_information['end_time_sec'], tz=datetime.timezone.utc)
        self.time = TimestampPair(_start_time, _end_time)

        self.modules = report.modules.keys()

        # TODO: Should I search in self.modules or in report.records?
        # ! All dfs are being materialised
        self.report = report
        self.posix_df = report.records['POSIX'].to_df() if 'POSIX' in self.modules else None
        self.stdio_df = report.records['STDIO'].to_df() if 'STDIO' in self.modules else None
        self.mpiio_df = report.records['MPI-IO'].to_df() if 'MPI-IO' in self.modules else None

        self.lustre_df = report.records['LUSTRE'].to_df() if 'LUSTRE' in self.modules else None

        self.dxt_posix = report.records['DXT_POSIX'].to_df() if 'DXT_POSIX' in self.modules else None
        self.dxt_mpiio = report.records['DXT_MPIIO'].to_df() if 'DXT_MPIIO' in self.modules else None

    def generate_dxt_posix_rw_df(self) -> None:
        if not args.backtrace:
            return
        if not self.dxt_posix:
            return
        if "address_line_mapping" not in self.dxt_posix:
            args.backtrace = False
            return

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

        for r in zip(self.dxt_posix['rank'], self.dxt_posix['read_segments'], self.dxt_posix['write_segments'],
                     self.dxt_posix['id']):
            if not r[1].empty:
                read_id.append([r[3]] * len((r[1]["length"].to_list())))
                read_rank.append([r[0]] * len((r[1]["length"].to_list())))
                read_length.append(r[1]["length"].to_list())
                read_end_time.append(r[1]["end_time"].to_list())
                read_start_time.append(r[1]["start_time"].to_list())
                read_operation.append(["read"] * len((r[1]["length"].to_list())))
                read_offsets.append(r[1]["offset"].to_list())

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

        self.dxt_posix_read_data = pd.DataFrame(
            {
                "id": read_id,
                "rank": read_rank,
                "length": read_length,
                "end_time": read_end_time,
                "start_time": read_start_time,
                "operation": read_operation,
                "offsets": read_offsets,
            }
        )

        self.dxt_posix_write_data = pd.DataFrame(
            {
                "id": write_id,
                "rank": write_rank,
                "length": write_length,
                "end_time": write_end_time,
                "start_time": write_start_time,
                "operation": write_operation,
                "offsets": write_offsets,
            }
        )

    def calculate_insights(self) -> None:
        self.total_write_size_stdio = self.stdio_df['counters']['STDIO_BYTES_WRITTEN'].sum() if self.stdio_df else 0
        self.total_read_size_stdio = self.stdio_df['counters']['STDIO_BYTES_READ'].sum() if self.stdio_df else 0
        self.total_size_stdio = self.total_write_size_stdio + self.total_read_size_stdio

        self.total_write_size_posix = self.posix_df['counters']['POSIX_BYTES_WRITTEN'].sum() if self.posix_df else 0
        self.total_read_size_posix = self.posix_df['counters']['POSIX_BYTES_READ'].sum() if self.posix_df else 0
        self.total_size_posix = self.total_write_size_posix + self.total_read_size_posix

        self.total_write_size_mpiio = self.mpiio_df['counters']['MPIIO_BYTES_WRITTEN'].sum() if self.mpiio_df else 0
        self.total_read_size_mpiio = self.mpiio_df['counters']['MPIIO_BYTES_READ'].sum() if self.mpiio_df else 0
        self.total_size_mpiio = self.total_write_size_mpiio + self.total_read_size_mpiio

        # POSIX will capture POSIX-only and MPI-IO
        if self.total_size_posix > 0 and self.total_size_posix >= self.total_size_mpiio:
            self.total_size_posix -= self.total_size_mpiio

        self.total_size = self.total_size_stdio + self.total_size_posix + self.total_size_mpiio

        assert (self.total_size_stdio >= 0)
        assert (self.total_size_posix >= 0)
        assert (self.total_size_mpiio >= 0)

    def files_stuff(self) -> None:
        file_map = self.report.name_records

        self.total_files = len(file_map)

        # files = dict()

        for id, path in file_map.items():
            uses_stdio = len(
                self.stdio_df['counters'][self.stdio_df['counters']['id'] == id]) > 0 if self.stdio_df else 0
            uses_posix = len(
                self.posix_df['counters'][self.posix_df['counters']['id'] == id]) > 0 if self.posix_df else 0
            uses_mpiio = len(
                self.mpiio_df['counters'][self.mpiio_df['counters']['id'] == id]) > 0 if self.mpiio_df else 0

            self.total_files_stdio += uses_stdio
            self.total_files_posix += uses_posix
            self.total_files_mpiio += uses_mpiio

            self.files[id] = {
                'path': path,
                'stdio': uses_stdio,
                'posix': uses_posix,
                'mpiio': uses_mpiio
            }

    def check_stdio(self) -> None:
        module.check_stdio(self.total_size, self.total_size_stdio)

    def check_mpiio(self) -> None:
        module.check_mpiio(self.modules)

    def something(self) -> None:
        if not self.posix_df:
            return

        self.total_reads = self.posix_df['counters']['POSIX_READS'].sum()
        self.total_writes = self.posix_df['counters']['POSIX_WRITES'].sum()
        self.total_operations = self.total_writes + self.total_reads

        module.check_operation_intensive(self.total_operations, self.total_reads, self.total_writes)

        self.total_read_size = self.posix_df['counters']['POSIX_BYTES_READ'].sum()
        self.total_written_size = self.posix_df['counters']['POSIX_BYTES_WRITTEN'].sum()
        self.total_size = self.total_written_size + self.total_read_size

        module.check_size_intensive(self.total_size, self.total_read_size, self.total_written_size)

        self.total_reads_small = (
                self.posix_df['counters']['POSIX_SIZE_READ_0_100'].sum() +
                self.posix_df['counters']['POSIX_SIZE_READ_100_1K'].sum() +
                self.posix_df['counters']['POSIX_SIZE_READ_1K_10K'].sum() +
                self.posix_df['counters']['POSIX_SIZE_READ_10K_100K'].sum() +
                self.posix_df['counters']['POSIX_SIZE_READ_100K_1M'].sum()
        )
        self.total_writes_small = (
                self.posix_df['counters']['POSIX_SIZE_WRITE_0_100'].sum() +
                self.posix_df['counters']['POSIX_SIZE_WRITE_100_1K'].sum() +
                self.posix_df['counters']['POSIX_SIZE_WRITE_1K_10K'].sum() +
                self.posix_df['counters']['POSIX_SIZE_WRITE_10K_100K'].sum() +
                self.posix_df['counters']['POSIX_SIZE_WRITE_100K_1M'].sum()
        )

    def something2(self):
        detected_files = pd.DataFrame(self.posix_df['counters'].groupby('id')[['INSIGHTS_POSIX_SMALL_READ',
                                                                    'INSIGHTS_POSIX_SMALL_WRITE']].sum()).reset_index()
        detected_files.columns = ['id', 'total_reads', 'total_writes']
        detected_files.loc[:, 'id'] = detected_files.loc[:, 'id'].astype(str)

        file_map = self.report.name_records
        module.check_small_operation(self.total_reads, self.total_reads_small, self.total_writes, self.total_writes_small,
                                     detected_files,
                                     self.modules, file_map, self.dxt_posix, self.dxt_posix_read_data,
                                     self.dxt_posix_write_data)


def file_reader(trace_path: str):
    log = darshanll.log_open(args.log_path)

    modules = darshanll.log_get_modules(log)

    information = darshanll.log_get_job(log)


def log_relation_check():
    # TODO: Ensure that all logs are from a single job, generated at the same time, from the same executable and using the same library version
    pass


def handler():
    console = init_console()

    insights_start_time = time.time()

    # TODO: Break here for new fn

    trace_path = args.log_paths[0]  # TODO: A single file rn

    darshan.enable_experimental()
    library_version = darshanll.get_lib_version()

    # TODO: Can this be put in a with block?
    log = darshanll.log_open(trace_path)
    information = darshanll.log_get_job(log)
    darshanll.log_close(log)

    report = darshan.DarshanReport(trace_path)
    current_trace = DarshanTrace(trace_path, information, report)  # WIP: Implement this constructor
    #

    # TODO: What to do here?
    # # Make sure log format is of the same version
    # filename = args.log_path
    # # check_log_version(console, args.log_path, log_version, library_version)
    #

    # TODO: Break here

    #########################################################################################################################################################################

    # TODO: Check usage of STDIO, POSIX, and MPI-IO per file

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

    # Since POSIX will capture both POSIX-only accesses and those comming from MPI-IO, we can subtract those
    if total_size_posix > 0 and total_size_posix >= total_size_mpiio:
        total_size_posix -= total_size_mpiio

    total_size = total_size_stdio + total_size_posix + total_size_mpiio

    assert (total_size_stdio >= 0)
    assert (total_size_posix >= 0)
    assert (total_size_mpiio >= 0)

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

    check_stdio(total_size, total_size_stdio)
    check_mpiio(modules)

    #########################################################################################################################################################################

    if 'POSIX' in report.records:
        df = report.records['POSIX'].to_df()

        #########################################################################################################################################################################

        # Get number of write/read operations
        total_reads = df['counters']['POSIX_READS'].sum()
        total_writes = df['counters']['POSIX_WRITES'].sum()

        # Get total number of I/O operations
        total_operations = total_writes + total_reads

        # To check whether the application is write-intersive or read-intensive we only look at the POSIX level and check if the difference between reads and writes is larger than 10% (for more or less), otherwise we assume a balance
        check_operation_intensive(total_operations, total_reads, total_writes)

        total_read_size = df['counters']['POSIX_BYTES_READ'].sum()
        total_written_size = df['counters']['POSIX_BYTES_WRITTEN'].sum()

        total_size = total_written_size + total_read_size

        check_size_intensive(total_size, total_read_size, total_written_size)

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

        detected_files = pd.DataFrame(df['counters'].groupby('id')[['INSIGHTS_POSIX_SMALL_READ',
                                                                    'INSIGHTS_POSIX_SMALL_WRITE']].sum()).reset_index()
        detected_files.columns = ['id', 'total_reads', 'total_writes']
        detected_files.loc[:, 'id'] = detected_files.loc[:, 'id'].astype(str)

        check_small_operation(total_reads, total_reads_small, total_writes, total_writes_small, detected_files, modules,
                              file_map, dxt_posix, dxt_posix_read_data, dxt_posix_write_data)

        #########################################################################################################################################################################

        # How many requests are misaligned?

        total_mem_not_aligned = df['counters']['POSIX_MEM_NOT_ALIGNED'].sum()
        total_file_not_aligned = df['counters']['POSIX_FILE_NOT_ALIGNED'].sum()

        check_misaligned(total_operations, total_mem_not_aligned, total_file_not_aligned, modules, file_map, df_lustre,
                         dxt_posix, dxt_posix_read_data)

        #########################################################################################################################################################################

        # Redundant read-traffic (based on Phill)
        # POSIX_MAX_BYTE_READ (Highest offset in the file that was read)
        max_read_offset = df['counters']['POSIX_MAX_BYTE_READ'].max()
        max_write_offset = df['counters']['POSIX_MAX_BYTE_WRITTEN'].max()

        check_traffic(max_read_offset, total_read_size, max_write_offset, total_written_size, dxt_posix,
                      dxt_posix_read_data, dxt_posix_write_data)

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

        check_random_operation(read_consecutive, read_sequential, read_random, total_reads, write_consecutive,
                               write_sequential, write_random, total_writes, dxt_posix, dxt_posix_read_data,
                               dxt_posix_write_data)

        #########################################################################################################################################################################

        # Shared file with small operations

        shared_files = df['counters'].loc[(df['counters']['rank'] == -1)]

        shared_files = shared_files.assign(id=lambda d: d['id'].astype(str))

        if not shared_files.empty:
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

            check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes,
                                         total_shared_writes_small, shared_files, file_map)

        #########################################################################################################################################################################

        count_long_metadata = len(
            df['fcounters'][(df['fcounters']['POSIX_F_META_TIME'] > thresholds['metadata_time_rank'][0])])

        check_long_metadata(count_long_metadata, modules)

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

            if total_transfer_size and abs(
                    row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size > \
                    thresholds['imbalance_stragglers'][0]:
                stragglers_count += 1

                detected_files.append([
                    row['id'],
                    abs(row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size * 100
                ])

        column_names = ['id', 'data_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        check_shared_data_imblance(stragglers_count, detected_files, file_map, dxt_posix, dxt_posix_read_data,
                                   dxt_posix_write_data)

        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME

        shared_files_times = df['fcounters'].loc[(df['fcounters']['rank'] == -1)]

        # Get the files responsible
        detected_files = []

        stragglers_count = 0
        stragglers_imbalance = {}

        shared_files_times = shared_files_times.assign(id=lambda d: d['id'].astype(str))

        for index, row in shared_files_times.iterrows():
            total_transfer_time = row['POSIX_F_WRITE_TIME'] + row['POSIX_F_READ_TIME'] + row['POSIX_F_META_TIME']

            if total_transfer_time and abs(
                    row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time > \
                    thresholds['imbalance_stragglers'][0]:
                stragglers_count += 1

                detected_files.append([
                    row['id'],
                    abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time * 100
                ])

        column_names = ['id', 'time_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        check_shared_time_imbalance(stragglers_count, detected_files, file_map)

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
            if row['POSIX_BYTES_WRITTEN_max'] and abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / \
                    row['POSIX_BYTES_WRITTEN_max'] > thresholds['imbalance_size'][0]:
                imbalance_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row[
                        'POSIX_BYTES_WRITTEN_max'] * 100
                ])

        column_names = ['id', 'write_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        check_individual_write_imbalance(imbalance_count, detected_files, file_map, dxt_posix, dxt_posix_write_data)

        imbalance_count = 0

        detected_files = []

        for index, row in aggregated.iterrows():
            if row['POSIX_BYTES_READ_max'] and abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row[
                'POSIX_BYTES_READ_max'] > thresholds['imbalance_size'][0]:
                imbalance_count += 1

                detected_files.append([
                    row['id'],
                    abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] * 100
                ])

        column_names = ['id', 'read_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        check_individual_read_imbalance(imbalance_count, detected_files, file_map, dxt_posix, dxt_posix_read_data)

    #########################################################################################################################################################################

    if 'MPI-IO' in report.records:
        # Check if application uses MPI-IO and collective operations
        df_mpiio = report.records['MPI-IO'].to_df()

        df_mpiio['counters'] = df_mpiio['counters'].assign(id=lambda d: d['id'].astype(str))

        # Get the files responsible
        detected_files = []

        df_mpiio_collective_reads = df_mpiio['counters']  #.loc[(df_mpiio['counters']['MPIIO_COLL_READS'] > 0)]

        total_mpiio_read_operations = df_mpiio['counters']['MPIIO_INDEP_READS'].sum() + df_mpiio['counters'][
            'MPIIO_COLL_READS'].sum()

        mpiio_coll_reads = df_mpiio['counters']['MPIIO_COLL_READS'].sum()
        mpiio_indep_reads = df_mpiio['counters']['MPIIO_INDEP_READS'].sum()

        detected_files = []
        if mpiio_coll_reads == 0 and total_mpiio_read_operations and total_mpiio_read_operations > \
                thresholds['collective_operations_absolute'][0]:
            files = pd.DataFrame(df_mpiio_collective_reads.groupby('id').sum()).reset_index()
            for index, row in df_mpiio_collective_reads.iterrows():
                if ((row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and
                        row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        thresholds['collective_operations'][0] and
                        (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        thresholds['collective_operations_absolute'][0]):
                    detected_files.append([
                        row['id'], row['MPIIO_INDEP_READS'],
                        row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100
                    ])

        column_names = ['id', 'absolute_indep_reads', 'percent_indep_reads']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_mpi_collective_read_operation(mpiio_coll_reads, mpiio_indep_reads, total_mpiio_read_operations,
                                            detected_files, file_map, dxt_mpiio)

        df_mpiio_collective_writes = df_mpiio['counters']  #.loc[(df_mpiio['counters']['MPIIO_COLL_WRITES'] > 0)]

        total_mpiio_write_operations = df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum() + df_mpiio['counters'][
            'MPIIO_COLL_WRITES'].sum()

        mpiio_coll_writes = df_mpiio['counters']['MPIIO_COLL_WRITES'].sum()
        mpiio_indep_writes = df_mpiio['counters']['MPIIO_INDEP_WRITES'].sum()

        detected_files = []
        if mpiio_coll_writes == 0 and total_mpiio_write_operations and total_mpiio_write_operations > \
                thresholds['collective_operations_absolute'][0]:
            files = pd.DataFrame(df_mpiio_collective_writes.groupby('id').sum()).reset_index()

            for index, row in df_mpiio_collective_writes.iterrows():
                if ((row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and
                        row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        thresholds['collective_operations'][0] and
                        (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        thresholds['collective_operations_absolute'][0]):
                    detected_files.append([
                        row['id'], row['MPIIO_INDEP_WRITES'],
                        row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100
                    ])

        column_names = ['id', 'absolute_indep_writes', 'percent_indep_writes']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_mpi_collective_write_operation(mpiio_coll_writes, mpiio_indep_writes, total_mpiio_write_operations,
                                             detected_files, file_map, dxt_mpiio)

        #########################################################################################################################################################################

        # Look for usage of non-block operations

        # Look for HDF5 file extension

        has_hdf5_extension = False

        for index, row in df_mpiio['counters'].iterrows():
            if file_map[int(row['id'])].endswith('.h5') or file_map[int(row['id'])].endswith('.hdf5'):
                has_hdf5_extension = True

        mpiio_nb_reads = df_mpiio['counters']['MPIIO_NB_READS'].sum()
        mpiio_nb_writes = df_mpiio['counters']['MPIIO_NB_WRITES'].sum()

        check_mpi_none_block_operation(mpiio_nb_reads, mpiio_nb_writes, has_hdf5_extension, modules)

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
                        check_mpi_aggregator(cb_nodes, NUMBER_OF_COMPUTE_NODES)
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
                    os.path.basename(args.log_path)
                ),
                ' [b]EXECUTION TIME[/b]: [white]{} to {} ({:.2f} hours)[/white]'.format(
                    job_start,
                    job_end,
                    (job_end - job_start).total_seconds() / 3600
                ),
                ' [b]FILES[/b]:          [white]{} files ({} use STDIO, {} use POSIX, {} use MPI-IO)[/white]'.format(
                    total_files,
                    total_files_stdio,
                    total_files_posix - total_files_mpiio,
                    # Since MPI-IO files will always use POSIX, we can decrement to get a unique count
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

    display_content(console)
    display_thresholds(console)
    display_footer(console, insights_start_time, insights_end_time)

    # Export to HTML, SVG, and CSV
    trace_name = os.path.basename(args.log_path).replace('.darshan', '')
    out_dir = args.export_dir if args.export_dir != "" else os.getcwd()

    export_html(console, out_dir, trace_name)
    export_svg(console, out_dir, trace_name)
    export_csv(out_dir, trace_name, job['job']['jobid'])
