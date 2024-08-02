#!/usr/bin/env python3
import dataclasses
import io
import shlex
import shutil
import subprocess
import os
import sys
import typing
from dataclasses import dataclass
from typing import Optional

import darshan
import pandas as pd
import datetime
import darshan.backend.cffi_backend as darshanll
from packaging import version
from rich import print

import includes.config as config
import includes.module as module
from includes.module import *
from includes.parser import args


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

    ###
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
    ###
    max_read_offset: int
    max_write_offset: int
    ###

    stdio_df: pd.DataFrame = None
    posix_df: pd.DataFrame = None
    mpiio_df: pd.DataFrame = None
    lustre_df: pd.DataFrame = None

    dxt_posix: pd.DataFrame = None
    dxt_mpiio: pd.DataFrame = None

    dxt_posix_read_data: pd.DataFrame = None
    dxt_posix_write_data: pd.DataFrame = None

    total_files_stdio: int = 0
    total_files_posix: int = 0
    total_files_mpiio: int = 0

    files: dict[str, dict[str, int]] = dataclasses.field(default_factory=dict)

    total_reads: int = 0
    total_writes: int = 0
    total_operations: int = 0
    total_read_size: int = 0
    total_written_size: int = 0
    total_posix_size: int = 0
    total_reads_small: int = 0
    total_writes_small: int = 0

    total_mem_not_aligned: int = 0
    total_file_not_aligned: int = 0

    read_consecutive: int = 0
    read_sequential: int = 0
    read_random: int = 0
    write_consecutive: int = 0
    write_sequential: int = 0
    write_random: int = 0

    shared_files: pd.DataFrame = None
    total_shared_reads: int = 0
    total_shared_reads_small: int = 0
    total_shared_writes: int = 0
    total_shared_writes_small: int = 0

    count_long_metadata: int = 0

    posix_shared_data_imbalance_stragglers_count: int = 0

    # 2 functions (unsure ones)

    has_hdf5_extension: bool = False

    mpiio_nb_reads: int = 0
    mpiio_nb_writes: int = 0

    cb_nodes: Optional[int] = None
    number_of_compute_nodes: int = 0
    hints: list[str] = dataclasses.field(default_factory=list)

    job_start: Optional[datetime.datetime] = None
    job_end: Optional[datetime.datetime] = None

    aggregated: pd.DataFrame = None

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

        self.hints = []
        self.files = {}

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

        self.total_posix_size = self.total_size_stdio + self.total_size_posix + self.total_size_mpiio

        assert (self.total_size_stdio >= 0)
        assert (self.total_size_posix >= 0)
        assert (self.total_size_mpiio >= 0)

    def files_stuff(self) -> None:
        self.report.name_records = self.report.name_records

        self.total_files = len(self.report.name_records)

        # files = dict()

        for id, path in self.report.name_records.items():
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
        module.check_stdio(self.total_posix_size, self.total_size_stdio)

    def check_mpiio(self) -> None:
        module.check_mpiio(self.modules)

    def something(self) -> None:
        if not self.posix_df:
            return

        self.total_reads = self.posix_df['counters']['POSIX_READS'].sum()
        self.total_writes = self.posix_df['counters']['POSIX_WRITES'].sum()
        self.total_operations = self.total_writes + self.total_reads
        # ----------------------------------------------------------------------------------------------------------------------
        module.check_operation_intensive(self.total_operations, self.total_reads, self.total_writes)

        total_read_size = self.posix_df['counters']['POSIX_BYTES_READ'].sum()
        total_written_size = self.posix_df['counters']['POSIX_BYTES_WRITTEN'].sum()
        total_size = total_written_size + total_read_size

        module.check_size_intensive(total_size, total_read_size, total_written_size)
        # -----
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

    def small_operation_calculation(self):
        if not self.posix_df:
            return

        files = []

        self.posix_df['counters']['INSIGHTS_POSIX_SMALL_READ'] = (
                self.posix_df['counters']['POSIX_SIZE_READ_0_100'] +
                self.posix_df['counters']['POSIX_SIZE_READ_100_1K'] +
                self.posix_df['counters']['POSIX_SIZE_READ_1K_10K'] +
                self.posix_df['counters']['POSIX_SIZE_READ_10K_100K'] +
                self.posix_df['counters']['POSIX_SIZE_READ_100K_1M']
        )

        self.posix_df['counters']['INSIGHTS_POSIX_SMALL_WRITE'] = (
                self.posix_df['counters']['POSIX_SIZE_WRITE_0_100'] +
                self.posix_df['counters']['POSIX_SIZE_WRITE_100_1K'] +
                self.posix_df['counters']['POSIX_SIZE_WRITE_1K_10K'] +
                self.posix_df['counters']['POSIX_SIZE_WRITE_10K_100K'] +
                self.posix_df['counters']['POSIX_SIZE_WRITE_100K_1M']
        )

        detected_files = pd.DataFrame(self.posix_df['counters'].groupby('id')[['INSIGHTS_POSIX_SMALL_READ',
                                                                               'INSIGHTS_POSIX_SMALL_WRITE']].sum()).reset_index()
        detected_files.columns = ['id', 'total_reads',
                                  'total_writes']  # !: Rename later. total_small_reads, total_small_writes
        detected_files.loc[:, 'id'] = detected_files.loc[:, 'id'].astype(str)

        self.report.name_records = self.report.name_records
        module.check_small_operation(self.total_reads, self.total_reads_small, self.total_writes,
                                     self.total_writes_small,
                                     detected_files,
                                     self.modules, self.report.name_records, self.dxt_posix, self.dxt_posix_read_data,
                                     self.dxt_posix_write_data)

    def posix_alignment(self):
        if not self.posix_df:
            return

        self.total_mem_not_aligned = self.posix_df['counters']['POSIX_MEM_NOT_ALIGNED'].sum()
        self.total_file_not_aligned = self.posix_df['counters']['POSIX_FILE_NOT_ALIGNED'].sum()

        self.report.name_records = self.report.name_records
        module.check_misaligned(self.total_operations, self.total_mem_not_aligned, self.total_file_not_aligned,
                                self.modules, self.report.name_records, self.lustre_df, self.dxt_posix,
                                self.dxt_posix_read_data)

    def posix_redundant_reads(self):
        if not self.posix_df:
            return

        self.max_read_offset = self.posix_df['counters']['POSIX_MAX_BYTE_READ'].max()
        self.max_write_offset = self.posix_df['counters']['POSIX_MAX_BYTE_WRITTEN'].max()

        module.check_traffic(self.max_read_offset, self.total_read_size, self.max_write_offset, self.total_written_size,
                             self.dxt_posix, self.dxt_posix_read_data, self.dxt_posix_write_data)

    def posix_random_check(self):
        if not self.posix_df:
            return

        self.read_consecutive = self.posix_df['counters']['POSIX_CONSEC_READS'].sum()

        self.read_sequential = self.posix_df['counters']['POSIX_SEQ_READS'].sum()
        self.read_sequential -= self.read_consecutive

        self.read_random = self.total_reads - self.read_consecutive - self.read_sequential

        self.write_consecutive = self.posix_df['counters']['POSIX_CONSEC_WRITES'].sum()

        self.write_sequential = self.posix_df['counters']['POSIX_SEQ_WRITES'].sum()
        self.write_sequential -= self.write_consecutive

        self.write_random = self.total_writes - self.write_consecutive - self.write_sequential

        module.check_random_operation(self.read_consecutive, self.read_sequential, self.read_random, self.total_reads,
                                      self.write_consecutive, self.write_sequential, self.write_random,
                                      self.total_writes, self.dxt_posix,
                                      self.dxt_posix_read_data, self.dxt_posix_write_data)

    def posix_shared_file(self):
        if not self.posix_df:
            return

        self.shared_files = self.posix_df['counters'].loc[(self.posix_df['counters']['rank'] == -1)]

        self.shared_files = self.shared_files.assign(id=lambda d: d['id'].astype(str))

        if not self.shared_files.empty:
            self.total_shared_reads = self.shared_files['POSIX_READS'].sum()
            self.total_shared_reads_small = (
                    self.shared_files['POSIX_SIZE_READ_0_100'].sum() +
                    self.shared_files['POSIX_SIZE_READ_100_1K'].sum() +
                    self.shared_files['POSIX_SIZE_READ_1K_10K'].sum() +
                    self.shared_files['POSIX_SIZE_READ_10K_100K'].sum() +
                    self.shared_files['POSIX_SIZE_READ_100K_1M'].sum()
            )

            self.shared_files['INSIGHTS_POSIX_SMALL_READS'] = (
                    self.shared_files['POSIX_SIZE_READ_0_100'] +
                    self.shared_files['POSIX_SIZE_READ_100_1K'] +
                    self.shared_files['POSIX_SIZE_READ_1K_10K'] +
                    self.shared_files['POSIX_SIZE_READ_10K_100K'] +
                    self.shared_files['POSIX_SIZE_READ_100K_1M']
            )

            self.total_shared_writes = self.shared_files['POSIX_WRITES'].sum()
            self.total_shared_writes_small = (
                    self.shared_files['POSIX_SIZE_WRITE_0_100'].sum() +
                    self.shared_files['POSIX_SIZE_WRITE_100_1K'].sum() +
                    self.shared_files['POSIX_SIZE_WRITE_1K_10K'].sum() +
                    self.shared_files['POSIX_SIZE_WRITE_10K_100K'].sum() +
                    self.shared_files['POSIX_SIZE_WRITE_100K_1M'].sum()
            )

            self.shared_files['INSIGHTS_POSIX_SMALL_WRITES'] = (
                    self.shared_files['POSIX_SIZE_WRITE_0_100'] +
                    self.shared_files['POSIX_SIZE_WRITE_100_1K'] +
                    self.shared_files['POSIX_SIZE_WRITE_1K_10K'] +
                    self.shared_files['POSIX_SIZE_WRITE_10K_100K'] +
                    self.shared_files['POSIX_SIZE_WRITE_100K_1M']
            )

            self.report.name_records = self.report.name_records
            check_shared_small_operation(self.total_shared_reads, self.total_shared_reads_small,
                                         self.total_shared_writes,
                                         self.total_shared_writes_small, self.shared_files, self.report.name_records)

    def posix_long_metadata(self):
        if not self.posix_df:
            return

        self.count_long_metadata = len(
            self.posix_df['fcounters'][
                (self.posix_df['fcounters']['POSIX_F_META_TIME'] > config.thresholds['metadata_time_rank'][0])])

        module.check_long_metadata(self.count_long_metadata, self.modules)

    def posix_stragglers(self):
        if not self.posix_df:
            return
        # We already have a single line for each shared-file access
        # To check for stragglers, we can check the difference between the

        # POSIX_FASTEST_RANK_BYTES
        # POSIX_SLOWEST_RANK_BYTES
        # POSIX_F_VARIANCE_RANK_BYTES

        self.shared_files = self.shared_files.assign(id=lambda d: d['id'].astype(str))

        posix_straggler_files = []

        for index, row in self.shared_files.iterrows():
            total_transfer_size = row['POSIX_BYTES_WRITTEN'] + row['POSIX_BYTES_READ']

            if total_transfer_size and abs(
                    row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size > \
                    thresholds['imbalance_stragglers'][0]:
                self.posix_shared_data_imbalance_stragglers_count += 1

                posix_straggler_files.append([
                    row['id'],
                    abs(row['POSIX_SLOWEST_RANK_BYTES'] - row['POSIX_FASTEST_RANK_BYTES']) / total_transfer_size * 100
                ])

        column_names = ['id', 'data_imbalance']
        posix_straggler_files = pd.DataFrame(posix_straggler_files, columns=column_names)

        self.report.name_records = self.report.name_records
        module.check_shared_data_imblance(self.posix_shared_data_imbalance_stragglers_count, posix_straggler_files,
                                          self.report.name_records, self.dxt_posix,
                                          self.dxt_posix_read_data,
                                          self.dxt_posix_write_data)

        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME

    #################################################################################################################

    def posix_stragglers2(self):
        # Get the files responsible

        shared_files_times = self.posix_df['fcounters'].loc[(self.posix_df['fcounters']['rank'] == -1)]

        posix_shared_time_imbalance_detected_files = []

        posix_stragglers_shared_file_time_imbalance_count = 0
        posix_stragglers_shared_file_time_imbalance = {}

        shared_files_times = shared_files_times.assign(id=lambda d: d['id'].astype(str))

        for index, row in shared_files_times.iterrows():
            total_transfer_time = row['POSIX_F_WRITE_TIME'] + row['POSIX_F_READ_TIME'] + row['POSIX_F_META_TIME']

            if total_transfer_time and abs(
                    row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time > \
                    config.thresholds['imbalance_stragglers'][0]:
                posix_stragglers_shared_file_time_imbalance_count += 1

                posix_shared_time_imbalance_detected_files.append([
                    row['id'],
                    abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time * 100
                ])

        column_names = ['id', 'time_imbalance']
        posix_shared_time_imbalance_detected_files = pd.DataFrame(posix_shared_time_imbalance_detected_files,
                                                                  columns=column_names)
        module.check_shared_time_imbalance(posix_stragglers_shared_file_time_imbalance_count,
                                           posix_shared_time_imbalance_detected_files, self.report.name_records)

    def posix_imbalance(self):
        aggregated = self.posix_df['counters'].loc[(self.posix_df['counters']['rank'] != -1)][
            ['rank', 'id', 'POSIX_BYTES_WRITTEN', 'POSIX_BYTES_READ']
        ].groupby('id', as_index=False).agg({
            'rank': 'nunique',
            'POSIX_BYTES_WRITTEN': ['sum', 'min', 'max'],
            'POSIX_BYTES_READ': ['sum', 'min', 'max']
        })
        aggregated.columns = list(map('_'.join, aggregated.columns.values))
        aggregated = aggregated.assign(id=lambda d: d['id_'].astype(str))
        self.aggregated = aggregated

        # Get the files responsible
        imbalance_count = 0

        posix_shared_time_imbalance_detected_files = []

        for index, row in self.aggregated.iterrows():
            if row['POSIX_BYTES_WRITTEN_max'] and abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / \
                    row['POSIX_BYTES_WRITTEN_max'] > config.thresholds['imbalance_size'][0]:
                imbalance_count += 1

                posix_shared_time_imbalance_detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row[
                        'POSIX_BYTES_WRITTEN_max'] * 100
                ])

        column_names = ['id', 'write_imbalance']
        posix_shared_time_imbalance_detected_files = pd.DataFrame(posix_shared_time_imbalance_detected_files,
                                                                  columns=column_names)
        module.check_individual_write_imbalance(imbalance_count, posix_shared_time_imbalance_detected_files,
                                                self.report.name_records, self.dxt_posix, self.dxt_posix_write_data)

    def mpiio_processing(self):
        if not self.mpiio_df:
            return

        self.mpiio_df['counters'] = self.mpiio_df['counters'].assign(
            id=lambda d: d['id'].astype(str))  # What does this do?

        # Get the files responsible
        detected_files = []

        df_mpiio_collective_reads = self.mpiio_df['counters']  # .loc[(df_mpiio['counters']['MPIIO_COLL_READS'] > 0)]

        total_mpiio_read_operations = self.mpiio_df['counters']['MPIIO_INDEP_READS'].sum() + self.mpiio_df['counters'][
            'MPIIO_COLL_READS'].sum()

        mpiio_coll_reads = self.mpiio_df['counters']['MPIIO_COLL_READS'].sum()
        mpiio_indep_reads = self.mpiio_df['counters']['MPIIO_INDEP_READS'].sum()

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
                                            detected_files, self.report.name_records, self.dxt_mpiio)

        df_mpiio_collective_writes = self.mpiio_df['counters']  # .loc[(df_mpiio['counters']['MPIIO_COLL_WRITES'] > 0)]

        total_mpiio_write_operations = self.mpiio_df['counters']['MPIIO_INDEP_WRITES'].sum() + \
                                       self.mpiio_df['counters'][
                                           'MPIIO_COLL_WRITES'].sum()

        mpiio_coll_writes = self.mpiio_df['counters']['MPIIO_COLL_WRITES'].sum()
        mpiio_indep_writes = self.mpiio_df['counters']['MPIIO_INDEP_WRITES'].sum()

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
                                             detected_files, self.report.name_records, self.dxt_mpiio)

    def posix_imbalance2(self):
        imbalance_count = 0

        posix_shared_time_imbalance_detected_files = []

        for index, row in self.aggregated.iterrows():
            if row['POSIX_BYTES_READ_max'] and abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row[
                'POSIX_BYTES_READ_max'] > thresholds['imbalance_size'][0]:
                imbalance_count += 1

                posix_shared_time_imbalance_detected_files.append([
                    row['id'],
                    abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] * 100
                ])

        column_names = ['id', 'read_imbalance']
        posix_shared_time_imbalance_detected_files = pd.DataFrame(posix_shared_time_imbalance_detected_files,
                                                                  columns=column_names)
        module.check_individual_read_imbalance(imbalance_count, posix_shared_time_imbalance_detected_files,
                                               self.report.name_records, self.dxt_posix, self.dxt_posix_read_data)

    def hdf5_check(self):
        if not self.mpiio_df:
            return

        self.report.name_records = self.report.name_records  # Will this be optimised via JIT? Nvm CPython doesn't have JIT lol
        for index, row in self.mpiio_df['counters'].iterrows():
            if self.report.name_records[int(row['id'])].endswith('.h5') or self.report.name_records[
                int(row['id'])].endswith('.hdf5'):
                self.has_hdf5_extension = True
                break  # Early exit

    def mpiio_non_blocking(self):
        if not self.mpiio_df:
            return

        self.mpiio_nb_reads = self.mpiio_df['counters']['MPIIO_NB_READS'].sum()
        self.mpiio_nb_writes = self.mpiio_df['counters']['MPIIO_NB_WRITES'].sum()

        module.check_mpi_none_block_operation(self.mpiio_nb_reads, self.mpiio_nb_writes, self.has_hdf5_extension,
                                              self.modules)

    def CHECKnumber_of_aggregators(self):
        hints = ''

        if 'h' in self.report.metadata['job']['metadata']:
            hints = self.report.metadata['job']['metadata']['h']

            if hints:
                hints = hints.split(';')

            self.hints = hints

        if 'MPI-IO' in self.modules:

            for hint in hints:
                if hint != 'no':
                    (key, value) = hint.split('=')

                if key == 'cb_nodes':
                    self.cb_nodes = value

            # Try to get the number of compute nodes from SLURM, if not found, set as information
            command = f'sacct --job {self.report.metadata["job"]["jobid"]} --format=JobID,JobIDRaw,NNodes,NCPUs --parsable2 --delimiter ","'

            arguments = shlex.split(command)

            try:
                result = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                if result.returncode == 0:
                    # We have successfully fetched the information from SLURM
                    db = csv.DictReader(io.StringIO(result.stdout.decode('utf-8')))

                    try:
                        first = next(db)

                        if 'NNodes' in first:
                            self.number_of_compute_nodes = first['NNodes']

                            # Do we have one MPI-IO aggregator per node?
                            module.check_mpi_aggregator(self.cb_nodes, self.number_of_compute_nodes)
                    except StopIteration:
                        pass
            except FileNotFoundError:
                pass

    def something_else(self):
        if 'start_time' in self.report.metadata['job']:
            self.job_start = datetime.datetime.fromtimestamp(self.report.metadata['job']['start_time'],
                                                             datetime.timezone.utc)
            self.job_end = datetime.datetime.fromtimestamp(self.report.metadata['job']['end_time'],
                                                           datetime.timezone.utc)
        else:
            self.job_start = datetime.datetime.fromtimestamp(self.report.metadata['job']['start_time_sec'],
                                                             datetime.timezone.utc)
            self.job_end = datetime.datetime.fromtimestamp(self.report.metadata['job']['end_time_sec'],
                                                           datetime.timezone.utc)


def log_relation_check():
    # TODO: Ensure that all logs are from a single job, generated at the same time, from the same executable and using the same library version
    pass


def handler():
    console = init_console()

    insights_start_time = time.time()

    log_path = args.log_paths[0]  # TODO: A single file rn
    log = darshanll.log_open(log_path)

    modules = darshanll.log_get_modules(log)

    information = darshanll.log_get_job(log)

    trace_path = args.log_paths[0]  # TODO: A single file rn

    darshan.enable_experimental()
    library_version = darshanll.get_lib_version()

    # TODO: Can this be put in a with block?
    log = darshanll.log_open(trace_path)
    information = darshanll.log_get_job(log)
    darshanll.log_close(log)

    report = darshan.DarshanReport(trace_path)
    current_trace = DarshanTrace(trace_path, information, report)
    #

    # Leave this as is for now
    # # Make sure log format is of the same version
    # filename = args.log_path
    # # check_log_version(console, args.log_path, log_version, library_version)
    #


    current_trace.generate_dxt_posix_rw_df()
    current_trace.calculate_insights()
    current_trace.files_stuff()
    current_trace.check_stdio()
    current_trace.check_mpiio()
    current_trace.something()
    current_trace.small_operation_calculation()
    current_trace.posix_alignment()
    current_trace.posix_redundant_reads()
    current_trace.posix_random_check()
    current_trace.posix_shared_file()
    current_trace.posix_long_metadata()
    current_trace.posix_stragglers()
    current_trace.posix_stragglers2()
    current_trace.posix_imbalance()
    current_trace.hdf5_check()
    current_trace.mpiio_non_blocking()
    current_trace.CHECKnumber_of_aggregators()
    current_trace.something_else()

    insights_end_time = time.time()

    # Version 3.4.1 of py-darshan changed the contents on what is reported in 'job'
    if 'start_time' in report.metadata['job']:
        job_start = datetime.datetime.fromtimestamp(report.metadata['job']['start_time'], datetime.timezone.utc)
        job_end = datetime.datetime.fromtimestamp(report.metadata['job']['end_time'], datetime.timezone.utc)
    else:
        job_start = datetime.datetime.fromtimestamp(report.metadata['job']['start_time_sec'], datetime.timezone.utc)
        job_end = datetime.datetime.fromtimestamp(report.metadata['job']['end_time_sec'], datetime.timezone.utc)

    console.print()

    console.print(
        Panel(
            '\n'.join([
                ' [b]JOB[/b]:            [white]{}[/white]'.format(
                    report.metadata['job']['jobid']
                ),
                ' [b]EXECUTABLE[/b]:     [white]{}[/white]'.format(
                    report.metadata['exe'].split()[0]
                ),
                ' [b]DARSHAN[/b]:        [white]{}[/white]'.format(
                    os.path.basename(log_path)
                ),
                ' [b]EXECUTION TIME[/b]: [white]{} to {} ({:.2f} hours)[/white]'.format(
                    job_start,
                    job_end,
                    (job_end - job_start).total_seconds() / 3600
                ),
                ' [b]FILES[/b]:          [white]{} files ({} use STDIO, {} use POSIX, {} use MPI-IO)[/white]'.format(
                    current_trace.total_files_posix,
                    current_trace.total_files_stdio,
                    current_trace.total_files_posix - current_trace.total_files_mpiio,
                    # Since MPI-IO files will always use POSIX, we can decrement to get a unique count
                    current_trace.total_files_mpiio
                ),
                ' [b]COMPUTE NODES[/b]   [white]{}[/white]'.format(
                    current_trace.number_of_compute_nodes
                ),
                ' [b]PROCESSES[/b]       [white]{}[/white]'.format(
                    report.metadata['job']['nprocs']
                ),
                ' [b]HINTS[/b]:          [white]{}[/white]'.format(
                    ' '.join(current_trace.hints)
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
    trace_name = os.path.basename(log_path).replace('.darshan', '')
    out_dir = args.export_dir if args.export_dir != "" else os.getcwd()

    export_html(console, out_dir, trace_name)
    export_svg(console, out_dir, trace_name)
    export_csv(out_dir, trace_name, report.metadata['job']['jobid'])
