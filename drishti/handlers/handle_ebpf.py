#!/usr/bin/env python3

import os
import time

import pandas as pd

from drishti.includes.module import *
from drishti.includes.parser_ebpf import build_dataframes

'''
process_helper() below started as a copy of handle_recorder.process_helper(),
since a .pfw trace parses into the same df_intervals / df_posix_records
schema Recorder produces. It's kept as an independent copy rather than a
shared import: eBPF captures more per-call detail (precise per-call
start/end, syscall args) than Recorder or Darshan ever have, and the plan
is to extend specific checks here without touching the Recorder path.

New checks with no Recorder/Darshan equivalent (E08-E11): I/O-compute
serialization, file reopen churn, sync overkill, and software-stack
overhead. All four need the per-call timeline.

Checks that have already diverged from the Recorder pipeline:
  - collective vs independent MPI-IO: replaced by the gated
    check_mpi_collective_operation_cost (in includes/module.py).
  - small operations: replaced by the cost-based
    check_small_operation_cost (in includes/module.py), which keys on
    time share rather than request count, excludes cache-served reads,
    attributes costly requests to the issuing library layer, and derives
    the "small" boundary from the trace.
'''


def handler():
    df_intervals = None
    df_posix_records = None
    df_layers = None
    df_file_map = None
    file_map = None

    cache_paths = {
        'intervals': args.log_path + '.intervals.csv',
        'records': args.log_path + '.records.csv',
        'layers': args.log_path + '.layers.csv',
        'filemap': args.log_path + '.filemap.csv',
    }

    if all(os.path.exists(p) for p in cache_paths.values()):
        for p in cache_paths.values():
            print('Using parsed file: {}'.format(os.path.abspath(p)))
        df_intervals = pd.read_csv(cache_paths['intervals'])
        df_posix_records = pd.read_csv(cache_paths['records'])
        df_layers = pd.read_csv(cache_paths['layers'])
        df_file_map = pd.read_csv(cache_paths['filemap'])
        file_map = {}
        for index, row in df_file_map.iterrows():
            file_map[row['file_id']] = row['file_name']
    else:
        df_intervals, df_posix_records, df_layers, file_map = build_dataframes(args.log_path)

        df_intervals.to_csv(cache_paths['intervals'], mode='w', index=False, header=True)
        df_posix_records.to_csv(cache_paths['records'], mode='w', index=False, header=True)
        df_layers.to_csv(cache_paths['layers'], mode='w', index=False, header=True)

        df_file_map = pd.DataFrame(list(file_map.items()), columns=['file_id', 'file_name'])
        df_file_map.to_csv(cache_paths['filemap'], mode='w', index=False, header=True)

    if args.split_files:
        for fid in file_map:
            process_helper(file_map, df_intervals[(df_intervals['file_id'] == fid)],
                           df_posix_records[(df_posix_records['fname'] == file_map[fid])], df_layers, fid)
    else:
        process_helper(file_map, df_intervals, df_posix_records, df_layers)


def process_helper(file_map, df_intervals, df_posix_records, df_layers, fid=None):
    # A run can be entirely metadata (opens, stats, locks) with no data
    # transfer at all, and that is exactly the case worth reporting on.
    # The Recorder pipeline bails out here; this path must not.
    if not len(df_intervals) and not len(df_posix_records): return

    insights_start_time = time.time()

    console = init_console()

    modules = set(df_intervals['api'].unique())
    # Check usage of POSIX, and MPI-IO per file
    total_size_stdio = 0
    total_size_posix = 0
    total_size_mpiio = 0
    total_size = 0

    total_files = len(file_map)
    total_files_stdio = 0
    total_files_posix = 0
    total_files_mpiio = 0

    if args.split_files:
        total_size_stdio = df_intervals[(df_intervals['api'] == 'STDIO')]['size'].sum()
        total_size_posix = df_intervals[(df_intervals['api'] == 'POSIX')]['size'].sum()
        total_size_mpiio = df_intervals[(df_intervals['api'] == 'MPI-IO')]['size'].sum()
    else:
        for id in file_map.keys():
            df_intervals_in_one_file = df_intervals[(df_intervals['file_id'] == id)]
            df_stdio_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'STDIO')]
            df_posix_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'POSIX')]
            df_mpiio_intervals_in_one_file = df_intervals_in_one_file[(df_intervals_in_one_file['api'] == 'MPI-IO')]

            if len(df_stdio_intervals_in_one_file):
                total_files_stdio += 1
                total_size_stdio += df_stdio_intervals_in_one_file['size'].sum()

            if len(df_posix_intervals_in_one_file):
                total_files_posix += 1
                total_size_posix += df_posix_intervals_in_one_file['size'].sum()

            if len(df_mpiio_intervals_in_one_file):
                total_files_mpiio += 1
                total_size_mpiio += df_mpiio_intervals_in_one_file['size'].sum()


    # Since POSIX will capture both POSIX-only accesses and those comming from MPI-IO, we can subtract those
    if total_size_posix > 0 and total_size_posix >= total_size_mpiio:
        total_size_posix -= total_size_mpiio

    total_size = total_size_stdio + total_size_posix + total_size_mpiio

    assert(total_size_stdio >= 0)
    assert(total_size_posix >= 0)
    assert(total_size_mpiio >= 0)

    check_stdio(total_size, total_size_stdio)
    check_mpiio(modules)

    #########################################################################################################################################################################

    # Checks that only a per-call timeline makes possible. These have no
    # Darshan/Recorder equivalent -- they need per-call timestamps, the
    # open/close pairing the parser builds, or the nesting of syscalls
    # inside library-layer intervals.

    check_metadata_categories(df_posix_records)
    check_io_compute_serialization(df_posix_records)
    check_file_reopen_churn(df_posix_records, file_map)
    check_sync_overkill(df_posix_records, df_intervals, file_map)
    check_software_stack_overhead(df_layers, df_posix_records)

    #########################################################################################################################################################################

    if df_intervals['api'].eq('POSIX').any():
        df_posix = df_intervals[(df_intervals['api'] == 'POSIX')]

        #########################################################################################################################################################################

        # Get number of write/read operations
        total_reads = len(df_posix[(df_posix['function'].str.contains('read'))])
        total_writes = len(df_posix[~(df_posix['function'].str.contains('read'))])

        # Get total number of I/O operations
        total_operations = total_writes + total_reads

        # To check whether the application is write-intersive or read-intensive we only look at the POSIX level and check if the difference between reads and writes is larger than 10% (for more or less), otherwise we assume a balance
        check_operation_intensive(total_operations, total_reads, total_writes)

        total_read_size = df_posix[(df_posix['function'].str.contains('read'))]['size'].sum()
        total_written_size = df_posix[~(df_posix['function'].str.contains('read'))]['size'].sum()

        total_size = total_written_size + total_read_size

        check_size_intensive(total_size, total_read_size, total_written_size)

        #########################################################################################################################################################################

        # Small operations: cost-based eBPF check (replaces the count-based
        # check_small_operation). Keys on time share, excludes likely
        # cache-served reads, attributes costly requests to the issuing
        # layer, and derives the "small" boundary from the trace itself.

        check_small_operation_cost(df_posix, df_layers, df_posix_records, file_map)

        #########################################################################################################################################################################

        # Misalignment, measured by cost rather than counted
        check_misaligned_cost(df_posix, file_map)

        #########################################################################################################################################################################

        # Redundant traffic, measured by interval union rather than
        # inferred from the highest offset touched
        check_traffic_redundancy(df_posix, file_map)

        #########################################################################################################################################################################

        # Check for a lot of random operations

        grp_posix_by_id = df_posix.groupby('file_id')

        read_consecutive = 0
        read_sequential = 0
        read_random = 0

        for id, df_filtered in grp_posix_by_id:
            df_filtered = df_filtered[(df_filtered['function'].str.contains('read'))].sort_values('start')

            for i in range(len(df_filtered) - 1):
                curr_interval = df_filtered.iloc[i]
                next_interval = df_filtered.iloc[i + 1]
                if curr_interval['offset'] + curr_interval['size'] == next_interval['offset']:
                    read_consecutive += 1
                elif curr_interval['offset'] + curr_interval['size'] < next_interval['offset']:
                    read_sequential += 1
                else:
                    read_random += 1

        write_consecutive = 0
        write_sequential = 0
        write_random = 0

        for id, df_filtered in grp_posix_by_id:
            df_filtered = df_filtered[~(df_filtered['function'].str.contains('read'))].sort_values('start')

            for i in range(len(df_filtered) - 1):
                curr_interval = df_filtered.iloc[i]
                next_interval = df_filtered.iloc[i + 1]
                if curr_interval['offset'] + curr_interval['size'] == next_interval['offset']:
                    write_consecutive += 1
                elif curr_interval['offset'] + curr_interval['size'] < next_interval['offset']:
                    write_sequential += 1
                else:
                    write_random += 1

        check_random_operation(read_consecutive, read_sequential, read_random, total_reads, write_consecutive, write_sequential, write_random, total_writes)

        #########################################################################################################################################################################

        # Shared file with small operations

        # A file is shared if it's been read/written by more than 1 rank
        detected_files = grp_posix_by_id['rank'].nunique()
        shared_files = set(detected_files[detected_files > 1].index)

        total_shared_reads = len(df_posix[(df_posix['file_id'].isin(shared_files)) & (df_posix['function'].str.contains('read'))])
        total_shared_reads_small = len(df_posix[(df_posix['file_id'].isin(shared_files))
                                    & (df_posix['function'].str.contains('read'))
                                    & (df_posix['size'] < thresholds['small_bytes'][0])])

        total_shared_writes = len(df_posix[(df_posix['file_id'].isin(shared_files)) & ~(df_posix['function'].str.contains('read'))])
        total_shared_writes_small = len(df_posix[(df_posix['file_id'].isin(shared_files))
                                    & ~(df_posix['function'].str.contains('read'))
                                    & (df_posix['size'] < thresholds['small_bytes'][0])])

        if args.split_files:
            detected_files = pd.DataFrame()
        else:
            detected_files = []
            for id in shared_files:
                read_cnt = len(df_posix[(df_posix['file_id'] == id)
                                        & (df_posix['function'].str.contains('read'))
                                        & (df_posix['size'] < thresholds['small_bytes'][0])])
                write_cnt = len(df_posix[(df_posix['file_id'] == id)
                                        & ~(df_posix['function'].str.contains('read'))
                                        & (df_posix['size'] < thresholds['small_bytes'][0])])
                detected_files.append([id, read_cnt, write_cnt])

            column_names = ['id', 'INSIGHTS_POSIX_SMALL_READS', 'INSIGHTS_POSIX_SMALL_WRITES']
            detected_files = pd.DataFrame(detected_files, columns=column_names)

        check_shared_small_operation(total_shared_reads, total_shared_reads_small, total_shared_writes, total_shared_writes_small, detected_files, file_map)

        #########################################################################################################################################################################

        # We already have a single line for each shared-file access
        # To check for stragglers, we can check the difference between the

        # POSIX_FASTEST_RANK_BYTES
        # POSIX_SLOWEST_RANK_BYTES
        # POSIX_VARIANCE_RANK_BYTES
        if args.split_files:
            if df_posix['rank'].nunique() > 1:
                total_transfer_size = df_posix['size'].sum()

                df_detected = df_posix.groupby('rank').agg({'size': 'sum', 'duration': 'sum'}).reset_index()
                slowest_rank_bytes = df_detected.loc[df_detected['duration'].idxmax(), 'size']
                fastest_rank_bytes = df_detected.loc[df_detected['duration'].idxmin(), 'size']

                check_shared_data_imblance_split(slowest_rank_bytes, fastest_rank_bytes, total_transfer_size)
        else:
            stragglers_count = 0

            detected_files = []
            for id in shared_files:
                df_posix_in_one_file = df_posix[(df_posix['file_id'] == id)]
                total_transfer_size = df_posix_in_one_file['size'].sum()

                df_detected = df_posix_in_one_file.groupby('rank').agg({'size': 'sum', 'duration': 'sum'}).reset_index()
                slowest_rank_bytes = df_detected.loc[df_detected['duration'].idxmax(), 'size']
                fastest_rank_bytes = df_detected.loc[df_detected['duration'].idxmin(), 'size']

                if total_transfer_size and abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size > thresholds['imbalance_stragglers'][0]:
                    stragglers_count += 1

                    detected_files.append([
                        id, abs(slowest_rank_bytes - fastest_rank_bytes) / total_transfer_size * 100
                    ])

            column_names = ['id', 'data_imbalance']
            detected_files = pd.DataFrame(detected_files, columns=column_names)

            check_shared_data_imblance(stragglers_count, detected_files, file_map)

        # POSIX_F_FASTEST_RANK_TIME
        # POSIX_F_SLOWEST_RANK_TIME
        # POSIX_F_VARIANCE_RANK_TIME
        if args.split_files:
            if df_posix['rank'].nunique() > 1:
                total_transfer_time = df_posix['duration'].sum()

                df_detected = df_posix.groupby('rank')['duration'].sum().reset_index()

                slowest_rank_time = df_detected['duration'].max()
                fastest_rank_time = df_detected['duration'].min()

                check_shared_time_imbalance_split(slowest_rank_time, fastest_rank_time, total_transfer_time)
        else:
            stragglers_count = 0

            detected_files = []
            for id in shared_files:
                df_posix_in_one_file = df_posix[(df_posix['file_id'] == id)]
                total_transfer_time = df_posix_in_one_file['duration'].sum()

                df_detected = df_posix_in_one_file.groupby('rank')['duration'].sum().reset_index()

                slowest_rank_time = df_detected['duration'].max()
                fastest_rank_time = df_detected['duration'].min()

                if total_transfer_time and abs(slowest_rank_time - fastest_rank_time) / total_transfer_time > thresholds['imbalance_stragglers'][0]:
                    stragglers_count += 1

                    detected_files.append([
                        id, abs(slowest_rank_time - fastest_rank_time) / total_transfer_time * 100
                    ])

            column_names = ['id', 'time_imbalance']
            detected_files = pd.DataFrame(detected_files, columns=column_names)

            check_shared_time_imbalance(stragglers_count, detected_files, file_map)

        # Get the individual files responsible for imbalance
        if args.split_files:
            if df_posix['rank'].nunique() == 1:
                df_detected = df_posix[~(df_posix['function'].str.contains('read'))]

                max_bytes_written = df_detected['size'].max()
                min_bytes_written = df_detected['size'].min()

                check_individual_write_imbalance_split(max_bytes_written, min_bytes_written)

            if df_posix['rank'].nunique() == 1:
                df_detected = df_posix[(df_posix['function'].str.contains('read'))]

                max_bytes_read = df_detected['size'].max()
                min_bytes_read = df_detected['size'].min()

                check_individual_read_imbalance_split(max_bytes_read, min_bytes_read)
        else:
            imbalance_count = 0

            detected_files = []
            for id in file_map.keys():
                if id in shared_files: continue
                df_detected = df_posix[(df_posix['file_id'] == id) & ~(df_posix['function'].str.contains('read'))]

                max_bytes_written = df_detected['size'].max()
                min_bytes_written = df_detected['size'].min()

                if max_bytes_written and abs(max_bytes_written - min_bytes_written) / max_bytes_written > thresholds['imbalance_size'][0]:
                    imbalance_count += 1

                    detected_files.append([
                        id, abs(max_bytes_written - min_bytes_written) / max_bytes_written  * 100
                    ])

            column_names = ['id', 'write_imbalance']
            detected_files = pd.DataFrame(detected_files, columns=column_names)

            check_individual_write_imbalance(imbalance_count, detected_files, file_map)

            imbalance_count = 0

            detected_files = []
            for id in shared_files:
                df_detected = df_posix[(df_posix['file_id'] == id) & (df_posix['function'].str.contains('read'))]

                max_bytes_read = df_detected['size'].max()
                min_bytes_read = df_detected['size'].min()

                if max_bytes_read and abs(max_bytes_read - min_bytes_read) / max_bytes_read > thresholds['imbalance_size'][0]:
                    imbalance_count += 1

                    detected_files.append([
                        id, abs(max_bytes_read - min_bytes_read) / max_bytes_read  * 100
                    ])

            column_names = ['id', 'read_imbalance']
            detected_files = pd.DataFrame(detected_files, columns=column_names)

            check_individual_read_imbalance(imbalance_count, detected_files, file_map)

    #########################################################################################################################################################################

    if df_intervals['api'].eq('MPI-IO').any():
        df_mpiio = df_intervals[(df_intervals['api'] == 'MPI-IO')]

        df_mpiio_reads = df_mpiio[(df_mpiio['function'].str.contains('read'))]
        df_mpiio_writes = df_mpiio[~(df_mpiio['function'].str.contains('read'))]

        # Collective vs independent: cost-based decision (replaces the
        # count-based check_mpi_collective_read/write_operation). Gates the
        # recommendation on whether the independent pattern is actually
        # aggregatable, whether the filesystem rewards aggregation, whether
        # rank imbalance makes the collective sync too expensive, and
        # whether there is enough data to amortize the fixed cost.

        check_mpi_collective_operation_cost(df_mpiio, df_intervals, file_map, args.fs_type)

        #########################################################################################################################################################################

        # Non-blocking I/O judged by the overlap it actually achieved,
        # not by whether a non-blocking call appears at all
        check_nonblocking_overlap(df_intervals, df_layers)

    #########################################################################################################################################################################

    # Aggregators identified from the trace: during a collective call
    # only the aggregator ranks issue POSIX I/O to the file
    check_mpi_aggregators(df_intervals, df_posix_records, file_map)

    #########################################################################################################################################################################

    insights_end_time = time.time()

    console.print()

    if args.split_files:
        console.print(
            Panel(
                '\n'.join([
                    ' [b]EBPF TRACE[/b]:    [white]{}[/white]'.format(
                        os.path.basename(args.log_path)
                    ),
                    ' [b]FILE[/b]:          [white]{} ({})[/white]'.format(
                        file_map[fid],
                        fid,
                    ),
                    ' [b]PROCESSES[/b]       [white]{}[/white]'.format(
                        df_intervals['rank'].nunique() if len(df_intervals)
                        else df_posix_records['rank'].nunique()
                    ),
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
    else:
        console.print(
            Panel(
                '\n'.join([
                    ' [b]EBPF TRACE[/b]:    [white]{}[/white]'.format(
                        os.path.basename(args.log_path)
                    ),
                    ' [b]FILES[/b]:          [white]{} files ({} use STDIO, {} use POSIX, {} use MPI-IO)[/white]'.format(
                        total_files,
                        total_files_stdio,
                        total_files_posix - total_files_mpiio,  # Since MPI-IO files will always use POSIX, we can decrement to get a unique count
                        total_files_mpiio
                    ),
                    ' [b]PROCESSES[/b]       [white]{}[/white]'.format(
                        df_intervals['rank'].nunique() if len(df_intervals)
                        else df_posix_records['rank'].nunique()
                    ),
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
    trace_name = os.path.splitext(os.path.basename(args.log_path))[0]
    if args.split_files:
        trace_name = f"{trace_name}.{fid}"
    out_dir = args.export_dir if args.export_dir != "" else os.getcwd()

    export_html(console, out_dir, trace_name)
    export_svg(console, out_dir, trace_name)
    export_csv(out_dir, trace_name)
