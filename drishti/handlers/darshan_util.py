import datetime
import typing
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import Dict, Final, Optional, Union

import pandas as pd


class ModuleType(str, Enum):
    """Enum for standard I/O module types"""
    POSIX = "posix"
    STDIO = "stdio"
    MPIIO = "mpiio"
    
    def __str__(self) -> str:
        return self.value


@dataclass
class TimeSpan:
    start: datetime.datetime
    end: datetime.datetime

    def __post_init__(self):
        if self.start > self.end:
            raise ValueError(f"TimeSpan start ({self.start}) must be <= end ({self.end})")

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
    sizes: Dict[Union[ModuleType, str], IOSize] = field(default_factory=dict)
    operations: Dict[Union[ModuleType, str], IOOperation] = field(default_factory=dict)

    def __post_init__(self):
        # Initialize standard modules if not present
        for module in ModuleType:
            # Ensure that the module is either in both sizes and operations or in neither
            assert (module in self.sizes) == (module in self.operations), f"Module {module} should be in both sizes and operations or in neither"

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
    def get_module_size(self, module: Optional[Union[ModuleType, str]] = None, data_type: Optional[str] = "total") -> int:
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

    def get_module_ops(self, module: Optional[Union[ModuleType, str]] = None, data_type: Optional[str] = "total") -> int:
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
    consecutive: ConsecutiveIOStats = field(default_factory=lambda: ConsecutiveIOStats(read=0, write=0), init=True)
    sequential: SequentialIOStats = field(default_factory=lambda: SequentialIOStats(read=0, write=0), init=True)
    random: RandomIOStats = field(default_factory=lambda: RandomIOStats(read=0, write=0), init=True)

@dataclass
class DarshanFile:
    # TODO: All fields which are not calculated should be instantly populated and not optional
    # TODO: Explore using typeddicts instead of dicts
    job_id: Optional[str] = None
    log_ver: Optional[str] = None
    time: Optional[TimeSpan] = None
    exe: Optional[str] = None
    modules: Optional[typing.Iterable[str]] = None
    name_records: Optional[typing.Dict[str, str]] = None
    max_read_offset: Optional[int] = None
    max_write_offset: Optional[int] = None
    total_files_stdio: Optional[int] = None
    total_files_posix: Optional[int] = None
    total_files_mpiio: Optional[int] = None
    files: Optional[typing.Dict[str, str]] = None
    
    # Replace individual I/O stats with IOStatistics class
    io_stats: Optional[IOStatistics] = None
    
    # File counts
    total_files: Optional[int] = 0
    
    # Additional I/O statistics organized by category
    small_io: Optional[SmallIOStats] = None
    
    # Direct alignment fields instead of a class
    mem_not_aligned: Optional[int] = None
    file_not_aligned: Optional[int] = None
    
    access_pattern: Optional[AccessPatternStats] = None
    
    # Use separate classes for shared operations
    shared_ops: Optional[SharedOpsStats] = None
    shared_small_ops: Optional[SharedSmallOpsStats] = None

    count_long_metadata: Optional[int] = None
    posix_shared_data_imbalance_stragglers_count: Optional[int] = None

    has_hdf5_extension: Optional[bool] = None

    mpiio_nb_ops: Optional[MPIIONonBlockingStats] = None

    cb_nodes: Optional[int] = None
    number_of_compute_nodes: Optional[int] = None
    hints: Optional[list[str]] = None

    timestamp: Optional[TimeSpan] = None

    aggregated: Optional[pd.DataFrame] = None

    mpi_coll_ops: Optional[MPICollectiveIOStats] = None
    mpi_indep_ops: Optional[MPIIndependentIOStats] = None

    detected_files_mpi_coll_reads: Optional[pd.DataFrame] = None
    detected_files_mpi_coll_writes: Optional[pd.DataFrame] = None

    imbalance_count_posix_shared_time: Optional[int] = None
    posix_shared_time_imbalance_detected_files: Optional[tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = None



