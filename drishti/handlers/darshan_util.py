import datetime
import typing
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from os import write
from typing import Dict, Final, Optional, Union, List, Tuple, Iterable

import numpy as np
import pandas as pd
from darshan import DarshanReport  # type: ignore
import drishti.includes.parser as parser
import drishti.includes.config as config


class ModuleType(str, Enum):
    """Enum for standard I/O module types"""

    POSIX = "POSIX"
    STDIO = "STDIO"
    MPIIO = "MPI-IO"

    def __str__(self) -> str:
        return self.value


@dataclass
class TimeSpan:
    start: datetime.datetime
    end: datetime.datetime

    def __post_init__(self):
        if self.start > self.end:
            raise ValueError(
                f"TimeSpan start ({self.start}) must be <= end ({self.end})"
            )


@dataclass
class IOCounter:
    """Base class for I/O metrics with read/write counts"""

    read: Final[int] = field(init=True)
    write: Final[int] = field(init=True)
    _total: Optional[int] = None

    @cached_property
    def total(self) -> int:
        """Total count, calculated once on first access"""
        if self._total is not None:
            return self._total
        return self.read + self.write


@dataclass
class IOSize(IOCounter):
    """Represents I/O size statistics in bytes"""

    pass


@dataclass
class IOOperation(IOCounter):
    """Represents I/O operation count statistics"""

    pass


@dataclass
class IOStatistics:
    """Tracks both I/O sizes and operations by module with aggregated metrics"""

    # Use dicts to store module-specific data
    sizes: Dict[ModuleType, IOSize] = field(init=True)
    operations: Dict[ModuleType, IOOperation] = field(init=True)

    def __post_init__(self):
        # Initialize standard modules if not present
        for module in ModuleType:
            # Ensure that the module is either in both sizes and operations or in neither
            assert (module in self.sizes) == (module in self.operations), (
                f"Module {module} should be in both sizes and operations or in neither"
            )

            if module not in self.sizes:
                self.sizes[module] = IOSize(read=0, write=0)
            if module not in self.operations:
                self.operations[module] = IOOperation(read=0, write=0)

    # Convenience properties for standard modules
    @cached_property
    def posix_size(self) -> int:
        return self.sizes[ModuleType.POSIX].total

    @cached_property
    def stdio_size(self) -> int:
        return self.sizes[ModuleType.STDIO].total

    @cached_property
    def mpiio_size(self) -> int:
        return self.sizes[ModuleType.MPIIO].total

    @cached_property
    def posix_ops(self) -> int:
        return self.operations[ModuleType.POSIX].total

    @cached_property
    def stdio_ops(self) -> int:
        return self.operations[ModuleType.STDIO].total

    @cached_property
    def mpiio_ops(self) -> int:
        return self.operations[ModuleType.MPIIO].total

    # Aggregated size properties
    @cached_property
    def read_bytes(self) -> int:
        """Total bytes read across all modules."""
        return sum(size.read for size in self.sizes.values())

    @cached_property
    def written_bytes(self) -> int:
        """Total bytes written across all modules."""
        return sum(size.write for size in self.sizes.values())

    @cached_property
    def total_bytes(self) -> int:
        """Total bytes transferred across all modules."""
        return self.read_bytes + self.written_bytes

    # Aggregated operation properties
    @cached_property
    def reads(self) -> int:
        """Total read operations across all modules."""
        return sum(op.read for op in self.operations.values())

    @cached_property
    def writes(self) -> int:
        """Total write operations across all modules."""
        return sum(op.write for op in self.operations.values())

    @cached_property
    def total_ops(self) -> int:
        """Total operations across all modules."""
        return self.reads + self.writes

    # Methods to get stats for specific modules
    def get_module_size(
        self,
        module: Optional[Union[ModuleType, str]] = None,
        data_type: Optional[str] = "total",
    ) -> int:
        """Get size statistics for a specific module or all modules if not specified."""
        if module is None and data_type is None:
            raise ValueError("Both module and data_type cannot be None")

        if module:
            if module not in self.sizes:
                raise ValueError(f"Module {module} not found in sizes")
            size = self.sizes[module]
            if data_type == "read":
                return size.read
            elif data_type == "write":
                return size.write
            else:  # data_type is None or "total"
                return size.total
        else:
            if data_type == "read":
                return self.read_bytes
            elif data_type == "write":
                return self.written_bytes
            else:  # data_type is None or "total"
                return self.total_bytes

    def get_module_ops(
        self,
        module: Optional[Union[ModuleType, str]] = None,
        data_type: Optional[str] = "total",
    ) -> int:
        """Get operation statistics for a specific module or all modules if not specified."""
        if module is None and data_type is None:
            raise ValueError("Both module and data_type cannot be None")

        if module:
            if module not in self.operations:
                raise ValueError(f"Module {module} not found in operations")
            ops = self.operations[module]
            if data_type == "read":
                return ops.read
            elif data_type == "write":
                return ops.write
            else:  # data_type is None or "total"
                return ops.total
        else:
            if data_type == "read":
                return self.reads
            elif data_type == "write":
                return self.writes
            else:  # data_type is None or "total"
                return self.total_ops


@dataclass
class SmallIOStats(IOCounter):
    """Statistics for small I/O operations"""

    pass  # Inherits read/write/total from IOCounter


@dataclass
class SharedOpsStats(IOCounter):
    """Statistics for shared file operations"""

    pass  # Inherits read/write/total from IOCounter


@dataclass
class SharedSmallOpsStats(IOCounter):
    """Statistics for small shared file operations"""

    pass  # Inherits read/write/total from IOCounter


@dataclass
class ConsecutiveIOStats(IOCounter):
    """Statistics for consecutive I/O operations"""

    pass  # Inherits read/write/total from IOCounter


@dataclass
class SequentialIOStats(IOCounter):
    """Statistics for sequential I/O operations"""

    pass  # Inherits read/write/total from IOCounter


@dataclass
class RandomIOStats(IOCounter):
    """Statistics for random I/O operations"""

    pass  # Inherits read/write/total from IOCounter


@dataclass
class MPIIONonBlockingStats(IOCounter):
    """Statistics for non-blocking MPI I/O operations"""

    pass


@dataclass
class MPICollectiveIOStats(IOCounter):
    """Statistics for collective MPI I/O operations"""

    pass


@dataclass
class MPIIndependentIOStats(IOCounter):
    """Statistics for independent MPI I/O operations"""

    pass


@dataclass
class AccessPatternStats:
    """Statistics for I/O access patterns by pattern type"""

    consecutive: ConsecutiveIOStats = field(
        default_factory=lambda: ConsecutiveIOStats(read=0, write=0), init=True
    )
    sequential: SequentialIOStats = field(
        default_factory=lambda: SequentialIOStats(read=0, write=0), init=True
    )
    random: RandomIOStats = field(
        default_factory=lambda: RandomIOStats(read=0, write=0), init=True
    )


@dataclass
class DarshanFile:
    # TODO: All fields which are not calculated should be instantly populated and not optional
    # TODO: Explore using typeddicts instead of dicts
    file_path: str
    _darshan_report: Optional[DarshanReport] = None
    job_id: Optional[str] = None
    log_ver: Optional[str] = None
    time: Optional[TimeSpan] = None
    exe: Optional[str] = None
    _modules: Optional[Iterable[str]] = None
    _name_records: Optional[Dict[int, str]] = None  # Keys are uint64
    _max_read_offset: Optional[int] = None
    _max_write_offset: Optional[int] = None
    total_files_stdio: Optional[int] = None
    total_files_posix: Optional[int] = None
    total_files_mpiio: Optional[int] = None
    files: Optional[Dict[str, str]] = None

    # Replace individual I/O stats with IOStatistics class
    _io_stats: Optional[IOStatistics] = None

    # File counts
    total_files: Optional[int] = 0

    # Additional I/O statistics organized by category
    _posix_small_io: Optional[SmallIOStats] = None

    _posix_detected_small_files: Optional[pd.DataFrame] = None

    # Direct alignment fields instead of a class
    _mem_not_aligned: Optional[int] = None
    _file_not_aligned: Optional[int] = None

    _posix_read_consecutive: Optional[int] = None
    _posix_write_consecutive: Optional[int] = None
    _posix_read_sequential: Optional[int] = None
    _posix_write_sequential: Optional[int] = None
    _posix_read_random: Optional[int] = None
    _posix_write_random: Optional[int] = None

    _posix_long_metadata_count: Optional[int] = None
    _posix_data_stragglers_count: Optional[int] = None
    _posix_time_stragglers_count: Optional[int] = None
    _posix_write_imbalance_count: Optional[int] = None
    _posix_read_imbalance_count: Optional[int] = None

    access_pattern: Optional[AccessPatternStats] = None

    # Use separate classes for shared operations
    _shared_ops: Optional[SharedOpsStats] = None
    shared_small_ops: Optional[SharedSmallOpsStats] = None

    count_long_metadata: Optional[int] = None
    posix_shared_data_imbalance_stragglers_count: Optional[int] = None

    _has_hdf5_extension: Optional[bool] = None

    _mpiio_nb_ops: Optional[MPIIONonBlockingStats] = None

    cb_nodes: Optional[int] = None
    number_of_compute_nodes: Optional[int] = None
    hints: Optional[List[str]] = None

    timestamp: Optional[TimeSpan] = None

    aggregated: Optional[pd.DataFrame] = None

    _mpi_coll_ops: Optional[MPICollectiveIOStats] = None
    _mpi_indep_ops: Optional[MPIIndependentIOStats] = None

    detected_files_mpi_coll_reads: Optional[pd.DataFrame] = None
    detected_files_mpi_coll_writes: Optional[pd.DataFrame] = None

    imbalance_count_posix_shared_time: Optional[int] = None
    posix_shared_time_imbalance_detected_files: Optional[
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
    ] = None

    @cached_property
    def report(self) -> DarshanReport:
        if self._darshan_report is None:
            self._darshan_report = DarshanReport(self.file_path)
        return self._darshan_report

    @cached_property
    def modules(self) -> Iterable[str]:
        if self._modules is None:
            self._modules = set(self.report.records.keys())
        return self._modules

    @cached_property
    def io_stats(self) -> IOStatistics:
        if self._io_stats is None:
            # Calculate I/O sizes
            sizes: Dict[ModuleType, IOSize] = {}
            ops: Dict[ModuleType, IOOperation] = {}
            if ModuleType.STDIO in self.modules:
                df = self.report.records[ModuleType.STDIO].to_df()
                counters = df["counters"]
                assert df, "STDIO module data frame is empty"

                stdio_read_size = counters["STDIO_BYTES_READ"].sum()
                stdio_write_size = counters["STDIO_BYTES_WRITTEN"].sum()
                sizes[ModuleType.STDIO] = IOSize(
                    read=stdio_read_size, write=stdio_write_size
                )

                stdio_read_ops = counters["STDIO_READS"].sum()
                stdio_write_ops = counters["STDIO_WRITES"].sum()
                ops[ModuleType.STDIO] = IOOperation(
                    read=stdio_read_ops, write=stdio_write_ops
                )

            if ModuleType.POSIX in self.modules:
                df = self.report.records[ModuleType.POSIX].to_df()
                counters = df["counters"]
                assert df, "POSIX module data frame is empty"

                posix_write_size = counters["POSIX_BYTES_WRITTEN"].sum()
                posix_read_size = counters["POSIX_BYTES_READ"].sum()
                sizes[ModuleType.POSIX] = IOSize(
                    read=posix_read_size, write=posix_write_size
                )

                posix_read_ops = counters["POSIX_READS"].sum()
                posix_write_ops = counters["POSIX_WRITES"].sum()
                ops[ModuleType.POSIX] = IOOperation(
                    read=posix_read_ops, write=posix_write_ops
                )

            if ModuleType.MPIIO in self.modules:
                df = self.report.records[ModuleType.MPIIO].to_df()
                counters = df["counters"]
                assert df, "MPIIO module data frame is empty"

                mpiio_write_size = counters["MPIIO_BYTES_WRITTEN"].sum()
                mpiio_read_size = counters["MPIIO_BYTES_READ"].sum()
                sizes[ModuleType.MPIIO] = IOSize(
                    read=mpiio_read_size, write=mpiio_write_size
                )

                mpiio_read_ops = counters['MPIIO_INDEP_READS'].sum() + counters['MPIIO_COLL_READS'].sum()
                mpiio_write_ops = counters['MPIIO_INDEP_WRITES'].sum() + counters['MPIIO_COLL_WRITES'].sum()
                ops[ModuleType.MPIIO] = IOOperation(
                    read=mpiio_read_ops, write=mpiio_write_ops
                )

            self._io_stats = IOStatistics(sizes=sizes, operations=ops)
        return self._io_stats

    @cached_property
    def posix_small_io(self) -> SmallIOStats:
        if self._posix_small_io is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            posix_reads_small = (
                posix_counters["POSIX_SIZE_READ_0_100"].sum()
                + posix_counters["POSIX_SIZE_READ_100_1K"].sum()
                + posix_counters["POSIX_SIZE_READ_1K_10K"].sum()
                + posix_counters["POSIX_SIZE_READ_10K_100K"].sum()
                + posix_counters["POSIX_SIZE_READ_100K_1M"].sum()
            )
            posix_writes_small = (
                posix_counters["POSIX_SIZE_WRITE_0_100"].sum()
                + posix_counters["POSIX_SIZE_WRITE_100_1K"].sum()
                + posix_counters["POSIX_SIZE_WRITE_1K_10K"].sum()
                + posix_counters["POSIX_SIZE_WRITE_10K_100K"].sum()
                + posix_counters["POSIX_SIZE_WRITE_100K_1M"].sum()
            )
            self._posix_small_io = SmallIOStats(
                read=posix_reads_small, write=posix_writes_small
            )
        return self._posix_small_io

    @property
    def posix_detected_small_files(self) -> pd.DataFrame:
        if self._posix_detected_small_files is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            posix_counters["INSIGHTS_POSIX_SMALL_READ"] = (
                posix_counters["POSIX_SIZE_READ_0_100"]
                + posix_counters["POSIX_SIZE_READ_100_1K"]
                + posix_counters["POSIX_SIZE_READ_1K_10K"]
                + posix_counters["POSIX_SIZE_READ_10K_100K"]
                + posix_counters["POSIX_SIZE_READ_100K_1M"]
            )
            posix_counters["INSIGHTS_POSIX_SMALL_WRITE"] = (
                posix_counters["POSIX_SIZE_WRITE_0_100"]
                + posix_counters["POSIX_SIZE_WRITE_100_1K"]
                + posix_counters["POSIX_SIZE_WRITE_1K_10K"]
                + posix_counters["POSIX_SIZE_WRITE_10K_100K"]
                + posix_counters["POSIX_SIZE_WRITE_100K_1M"]
            )
            detected_files = pd.DataFrame(
                posix_counters.groupby("id")[
                    ["INSIGHTS_POSIX_SMALL_READ", "INSIGHTS_POSIX_SMALL_WRITE"]
                ].sum()
            ).reset_index()
            detected_files.columns = pd.Index(["id", "total_reads", "total_writes"])
            detected_files.loc[:, "id"] = detected_files.loc[:, "id"].astype(str)
            self._posix_detected_small_files = detected_files
        return self._posix_detected_small_files

    @property
    def file_map(self) -> Dict[int, str]:
        return self.name_records

    @cached_property
    def name_records(self) -> Dict[int, str]:
        if self._name_records is None:
            self._name_records = self.report.name_records
        return self._name_records

    @property
    def dxt_posix_df(self) -> Optional[pd.DataFrame]:
        if parser.args.backtrace is False:
            return None
        assert "DXT_POSIX" in self.modules, "Missing DXT_POSIX module"
        dxt_posix_df = pd.DataFrame(self.report.records["DXT_POSIX"].to_df())
        return dxt_posix_df

    @property
    def dxt_posix_read_df(self) -> Optional[pd.DataFrame]:
        if parser.args.backtrace is False:
            return None
        assert "DXT_POSIX" in self.modules, "Missing DXT_POSIX module"
        df = self.dxt_posix_df
        assert df is not None, "Should be handled by parser.args.backtrace check"

        if "address_line_mapping" not in df:
            parser.args.backtrace = False
            return None

        read_id = []
        read_rank = []
        read_length = []
        read_offsets = []
        read_end_time = []
        read_start_time = []
        read_operation = []

        for r in zip(df["rank"], df["read_segments"], df["write_segments"], df["id"]):
            if not r[1].empty:
                read_id.append([r[3]] * len((r[1]["length"].to_list())))
                read_rank.append([r[0]] * len((r[1]["length"].to_list())))
                read_length.append(r[1]["length"].to_list())
                read_end_time.append(r[1]["end_time"].to_list())
                read_start_time.append(r[1]["start_time"].to_list())
                read_operation.append(["read"] * len((r[1]["length"].to_list())))
                read_offsets.append(r[1]["offset"].to_list())

        read_id = [element for nestedlist in read_id for element in nestedlist]
        read_rank = [element for nestedlist in read_rank for element in nestedlist]
        read_length = [element for nestedlist in read_length for element in nestedlist]
        read_offsets = [
            element for nestedlist in read_offsets for element in nestedlist
        ]
        read_end_time = [
            element for nestedlist in read_end_time for element in nestedlist
        ]
        read_operation = [
            element for nestedlist in read_operation for element in nestedlist
        ]
        read_start_time = [
            element for nestedlist in read_start_time for element in nestedlist
        ]

        dxt_posix_read_data = {
            "id": read_id,
            "rank": read_rank,
            "length": read_length,
            "end_time": read_end_time,
            "start_time": read_start_time,
            "operation": read_operation,
            "offsets": read_offsets,
        }

        return pd.DataFrame(dxt_posix_read_data)

    @property
    def dxt_posix_write_df(self) -> Optional[pd.DataFrame]:
        if parser.args.backtrace is False:
            return None
        assert "DXT_POSIX" in self.modules, "Missing DXT_POSIX module"
        df = self.dxt_posix_df
        assert df is not None, "Should be handled by parser.args.backtrace check"

        if "address_line_mapping" not in df:
            parser.args.backtrace = False
            return None

        write_id = []
        write_rank = []
        write_length = []
        write_offsets = []
        write_end_time = []
        write_start_time = []
        write_operation = []

        for r in zip(df["rank"], df["read_segments"], df["write_segments"], df["id"]):
            if not r[2].empty:
                write_id.append([r[3]] * len((r[2]["length"].to_list())))
                write_rank.append([r[0]] * len((r[2]["length"].to_list())))
                write_length.append(r[2]["length"].to_list())
                write_end_time.append(r[2]["end_time"].to_list())
                write_start_time.append(r[2]["start_time"].to_list())
                write_operation.append(["write"] * len((r[2]["length"].to_list())))
                write_offsets.append(r[2]["offset"].to_list())

        write_id = [element for nestedlist in write_id for element in nestedlist]
        write_rank = [element for nestedlist in write_rank for element in nestedlist]
        write_length = [
            element for nestedlist in write_length for element in nestedlist
        ]
        write_offsets = [
            element for nestedlist in write_offsets for element in nestedlist
        ]
        write_end_time = [
            element for nestedlist in write_end_time for element in nestedlist
        ]
        write_operation = [
            element for nestedlist in write_operation for element in nestedlist
        ]
        write_start_time = [
            element for nestedlist in write_start_time for element in nestedlist
        ]

        dxt_posix_write_data = pd.DataFrame(
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

        return pd.DataFrame(dxt_posix_write_data)

    @cached_property
    def mem_not_aligned(self) -> int:
        if self._mem_not_aligned is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._mem_not_aligned = posix_counters["POSIX_MEM_NOT_ALIGNED"].sum()
        return self._mem_not_aligned

    @cached_property
    def file_not_aligned(self) -> int:
        if self._file_not_aligned is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._file_not_aligned = posix_counters["POSIX_FILE_NOT_ALIGNED"].sum()
        return self._file_not_aligned

    @property
    def lustre_df(self) -> Optional[pd.DataFrame]:
        if "LUSTRE" not in self.modules:
            return None
        return pd.DataFrame(self.report.records["LUSTRE"].to_df())

    @cached_property
    def max_read_offset(self) -> int:
        if self._max_read_offset is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._max_read_offset = posix_counters["POSIX_MAX_BYTE_READ"].max()
        return self._max_read_offset

    @cached_property
    def max_write_offset(self) -> int:
        if self._max_write_offset is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._max_write_offset = posix_counters["POSIX_MAX_BYTE_WRITTEN"].max()
        return self._max_write_offset

    @cached_property
    def posix_read_consecutive(self) -> int:
        if self._posix_read_consecutive is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._posix_read_consecutive = posix_counters["POSIX_CONSEC_READS"].sum()
        return self._posix_read_consecutive

    @cached_property
    def posix_write_consecutive(self) -> int:
        if self._posix_write_consecutive is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._posix_write_consecutive = posix_counters["POSIX_CONSEC_WRITES"].sum()
        return self._posix_write_consecutive

    @cached_property
    def posix_read_sequential(self) -> int:
        if self._posix_read_sequential is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._posix_read_sequential = (
                posix_counters["POSIX_SEQ_READS"].sum() - self.posix_read_consecutive
            )
        return self._posix_read_sequential

    @cached_property
    def posix_write_sequential(self) -> int:
        if self._posix_write_sequential is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._posix_write_sequential = (
                posix_counters["POSIX_SEQ_WRITES"].sum() - self.posix_write_consecutive
            )
        return self._posix_write_sequential

    @cached_property
    def posix_read_random(self) -> int:
        if self._posix_read_random is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._posix_read_random = (
                self.io_stats.get_module_ops(ModuleType.POSIX, "read")
                - self.posix_read_consecutive
                - self.posix_read_sequential
            )
        return self._posix_read_random

    @cached_property
    def posix_write_random(self) -> int:
        if self._posix_write_random is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._posix_write_random = (
                self.io_stats.get_module_ops(ModuleType.POSIX, "write")
                - self.posix_write_consecutive
                - self.posix_write_sequential
            )
        return self._posix_write_random

    @property
    def posix_shared_files_df(self) -> pd.DataFrame:
        assert "POSIX" in self.modules, "Missing POSIX module"
        posix_df = self.report.records[ModuleType.POSIX].to_df()
        shared_files_df = posix_df["counters"].loc[(posix_df["counters"]["rank"] == -1)]
        shared_files_df = shared_files_df.assign(id=lambda d: d["id"].astype(str))
        return shared_files_df

    @cached_property
    def posix_shared_reads(self) -> int:
        if self._shared_ops is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._shared_ops = SharedOpsStats(
                read=posix_counters["POSIX_SHARED_READS"].sum(),
                write=posix_counters["POSIX_SHARED_WRITES"].sum(),
            )
        return self._shared_ops.read

    @cached_property
    def posix_shared_writes(self) -> int:
        if self._shared_ops is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_counters = posix_df["counters"]
            self._shared_ops = SharedOpsStats(
                read=posix_counters["POSIX_SHARED_READS"].sum(),
                write=posix_counters["POSIX_SHARED_WRITES"].sum(),
            )
        return self._shared_ops.write

    @cached_property
    def posix_long_metadata_count(self) -> int:
        if self._posix_long_metadata_count is None:
            posix_df = self.report.records[ModuleType.POSIX].to_df()
            posix_long_metadata_rows = posix_df["fcounters"][
                (
                    posix_df["fcounters"]["POSIX_F_META_TIME"]
                    > config.thresholds["metadata_time_rank"][0]
                )
            ]
            self._posix_long_metadata_count = len(posix_long_metadata_rows)
        return self._posix_long_metadata_count

    @property
    def posix_data_stragglers_df(self) -> pd.DataFrame:
        shared_files = self.posix_shared_files_df

        detected_files = []

        for index, row in shared_files.iterrows():
            total_transfer_size = row["POSIX_BYTES_WRITTEN"] + row["POSIX_BYTES_READ"]

            if (
                total_transfer_size
                and abs(
                    row["POSIX_SLOWEST_RANK_BYTES"] - row["POSIX_FASTEST_RANK_BYTES"]
                )
                / total_transfer_size
                > config.thresholds["imbalance_stragglers"][0]
            ):
                # stragglers_count += 1

                detected_files.append(
                    [
                        row["id"],
                        abs(
                            row["POSIX_SLOWEST_RANK_BYTES"]
                            - row["POSIX_FASTEST_RANK_BYTES"]
                        )
                        / total_transfer_size
                        * 100,
                    ]
                )

        column_names = ["id", "data_imbalance"]
        detected_files = pd.DataFrame(detected_files, columns=column_names)
        return detected_files

    @cached_property
    def posix_data_stragglers_count(self) -> int:
        if self._posix_data_stragglers_count is None:
            self._posix_data_stragglers_count = len(self.posix_data_stragglers_df)
        return self._posix_data_stragglers_count

    @property
    def posix_time_stragglers_df(self) -> pd.DataFrame:
        df = self.report.records[ModuleType.POSIX].to_df()

        shared_files_times = df['fcounters'].loc[(df['fcounters']['rank'] == -1)]

        # Get the files responsible
        detected_files = []

        # stragglers_count = 0
        # stragglers_imbalance = {}

        shared_files_times = shared_files_times.assign(id=lambda d: d['id'].astype(str))

        for index, row in shared_files_times.iterrows():
            total_transfer_time = row['POSIX_F_WRITE_TIME'] + row['POSIX_F_READ_TIME'] + row['POSIX_F_META_TIME']

            if total_transfer_time and abs(
                    row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time > \
                    config.thresholds['imbalance_stragglers'][0]:
                # stragglers_count += 1

                detected_files.append([
                    row['id'],
                    abs(row['POSIX_F_SLOWEST_RANK_TIME'] - row['POSIX_F_FASTEST_RANK_TIME']) / total_transfer_time * 100
                ])

        column_names = ['id', 'time_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        return detected_files

    @cached_property
    def posix_time_stragglers_count(self) -> int:
        if self._posix_time_stragglers_count is None:
            self._posix_time_stragglers_count = len(self.posix_time_stragglers_df)
        return self._posix_time_stragglers_count

    @property
    def posix_write_imbalance_df(self) -> pd.DataFrame:
        df = self.report.records[ModuleType.POSIX].to_df()

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
                    row['POSIX_BYTES_WRITTEN_max'] > config.thresholds['imbalance_size'][0]:
                imbalance_count += 1

                detected_files.append([
                    row['id'], abs(row['POSIX_BYTES_WRITTEN_max'] - row['POSIX_BYTES_WRITTEN_min']) / row[
                        'POSIX_BYTES_WRITTEN_max'] * 100
                ])

        column_names = ['id', 'write_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        return detected_files

    @cached_property
    def posix_write_imbalance_count(self) -> int:
        if self._posix_write_imbalance_count is None:
            self._posix_write_imbalance_count = len(self.posix_write_imbalance_df)
        return self._posix_write_imbalance_count

    @property
    def posix_read_imbalance_df(self) -> pd.DataFrame:
        df = self.report.records[ModuleType.POSIX].to_df()

        aggregated = df['counters'].loc[(df['counters']['rank'] != -1)][
            ['rank', 'id', 'POSIX_BYTES_WRITTEN', 'POSIX_BYTES_READ']
        ].groupby('id', as_index=False).agg({
            'rank': 'nunique',
            'POSIX_BYTES_WRITTEN': ['sum', 'min', 'max'],
            'POSIX_BYTES_READ': ['sum', 'min', 'max']
        })

        aggregated.columns = list(map('_'.join, aggregated.columns.values))

        aggregated = aggregated.assign(id=lambda d: d['id_'].astype(str))


        imbalance_count = 0

        detected_files = []

        for index, row in aggregated.iterrows():
            if row['POSIX_BYTES_READ_max'] and abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row[
                'POSIX_BYTES_READ_max'] > config.thresholds['imbalance_size'][0]:
                imbalance_count += 1

                detected_files.append([
                    row['id'],
                    abs(row['POSIX_BYTES_READ_max'] - row['POSIX_BYTES_READ_min']) / row['POSIX_BYTES_READ_max'] * 100
                ])

        column_names = ['id', 'read_imbalance']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        return detected_files

    @cached_property
    def posix_read_imbalance_count(self) -> int:
        if self._posix_read_imbalance_count is None:
            self._posix_read_imbalance_count = len(self.posix_read_imbalance_df)
        return self._posix_read_imbalance_count

    @cached_property
    def mpi_coll_ops(self) -> MPICollectiveIOStats:
        if self._mpi_coll_ops is None:
            mpi_df = self.report.records[ModuleType.MPIIO].to_df()
            mpi_coll_reads = mpi_df['counters']['MPIIO_COLL_READS'].sum()
            mpiio_coll_writes = mpi_df['counters']['MPIIO_COLL_WRITES'].sum()
            self._mpi_coll_ops = MPICollectiveIOStats(read=mpi_coll_reads, write=mpiio_coll_writes)
        return self._mpi_coll_ops

    @cached_property
    def mpi_indep_ops(self) -> MPIIndependentIOStats:
        if self._mpi_indep_ops is None:
            mpi_df = self.report.records[ModuleType.MPIIO].to_df()
            mpi_indep_reads = mpi_df['counters']['MPIIO_INDEP_READS'].sum()
            mpi_indep_writes = mpi_df['counters']['MPIIO_INDEP_WRITES'].sum()
            self._mpi_indep_ops = MPIIndependentIOStats(read=mpi_indep_reads, write=mpi_indep_writes)
        return self._mpi_indep_ops

    @property
    def mpi_read_df(self) -> pd.DataFrame:
        mpi_df = self.report.records[ModuleType.MPIIO].to_df()
        counters = mpi_df['counters']
        mpi_coll_reads = self.mpi_coll_ops.read
        mpi_total_reads = self.io_stats.get_module_ops(ModuleType.MPIIO, "read")

        detected_files = []

        if mpi_coll_reads == 0 and mpi_total_reads and mpi_total_reads > \
                config.thresholds['collective_operations_absolute'][0]:
            files = pd.DataFrame(counters.groupby('id').sum()).reset_index()
            for index, row in counters.iterrows():
                if ((row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and
                        row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        config.thresholds['collective_operations'][0] and
                        (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        config.thresholds['collective_operations_absolute'][0]):
                    detected_files.append([
                        row['id'], row['MPIIO_INDEP_READS'],
                        row['MPIIO_INDEP_READS'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100
                    ])

        column_names = ['id', 'absolute_indep_reads', 'percent_indep_reads']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        return detected_files

    @property
    def dxt_mpi_df(self) -> Optional[pd.DataFrame]:
        if not parser.args.backtrace:
            return None
        if "DXT_MPIIO" not in self.modules:
            return None

        dxt_mpiio = self.report.records["DXT_MPIIO"].to_df()
        dxt_mpiio = pd.DataFrame(dxt_mpiio)
        return dxt_mpiio

    @property
    def mpi_write_df(self) -> pd.DataFrame:
        mpi_df = self.report.records[ModuleType.MPIIO].to_df()
        counters = mpi_df['counters']

        mpi_coll_writes = self.mpi_coll_ops.write
        total_mpiio_write_operations = self.io_stats.get_module_ops(ModuleType.MPIIO, "write")


        detected_files = []
        if mpi_coll_writes == 0 and total_mpiio_write_operations and total_mpiio_write_operations > \
                config.thresholds['collective_operations_absolute'][0]:
            files = pd.DataFrame(counters.groupby('id').sum()).reset_index()

            for index, row in counters.iterrows():
                if ((row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) and
                        row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        config.thresholds['collective_operations'][0] and
                        (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) >
                        config.thresholds['collective_operations_absolute'][0]):
                    detected_files.append([
                        row['id'], row['MPIIO_INDEP_WRITES'],
                        row['MPIIO_INDEP_WRITES'] / (row['MPIIO_INDEP_READS'] + row['MPIIO_INDEP_WRITES']) * 100
                    ])

        column_names = ['id', 'absolute_indep_writes', 'percent_indep_writes']
        detected_files = pd.DataFrame(detected_files, columns=column_names)

        return detected_files

    @cached_property
    def mpiio_nb_ops(self) -> MPIIONonBlockingStats:
        if self._mpiio_nb_ops is None:
            mpi_df = self.report.records[ModuleType.MPIIO].to_df()
            mpi_nb_reads = mpi_df['counters']['MPIIO_NB_READS'].sum()
            mpi_nb_writes = mpi_df['counters']['MPIIO_NB_WRITES'].sum()
            self._mpiio_nb_ops = MPIIONonBlockingStats(read=mpi_nb_reads, write=mpi_nb_writes)
        return self._mpiio_nb_ops

    @cached_property
    def has_hdf5_extension(self) -> bool:
        if self._has_hdf5_extension is None:
            self._has_hdf5_extension = False
            mpi_df = self.report.records[ModuleType.MPIIO].to_df()
            for index, row in mpi_df['counters'].iterrows():
                if self.file_map[int(row['id'])].endswith('.h5') or self.file_map[int(row['id'])].endswith('.hdf5'):
                    self._has_hdf5_extension = True
                    # break
        return self._has_hdf5_extension
