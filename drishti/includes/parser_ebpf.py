#!/usr/bin/env python3

"""
Parser for Datacrumbs eBPF traces in Chrome/Perfetto Trace Event Format (.pfw).

Each line is a single JSON "complete" event (ph == "X"):
    {"id":..,"name":<function>,"cat":<category>,"ph":"X",
     "ts":<start_us>,"dur":<duration_us>,"pid":<pid>,"tid":<tid>,
     "args": {...}}

Unlike Recorder, filenames are never given directly on read/write events --
only a file descriptor is. Resolving fd -> filename requires walking each
process's events in timestamp order and tracking open/close pairs, since a
fd can be reused after close().

Note on units: Chrome/Perfetto Trace Event Format specifies 'ts' and 'dur'
in microseconds. Drishti's checks (e.g. thresholds['metadata_time_rank'],
compared directly against summed 'duration') assume seconds, matching
Darshan/Recorder. This parser converts to seconds so no downstream check
needs to know or care which trace format produced its input.
"""

import json

import pandas as pd

US_TO_S = 1e-6

# Categories observed in Datacrumbs traces
CAT_POSIX = 'sys'
CAT_MPIIO = 'mpi'

# Function name fragments that open a new fd (checked with a simple substring
# match, mirroring the style already used elsewhere in Drishti, e.g.
# handle_recorder.get_accessed_files / init_df_posix_recordes)
OPEN_FUNCTIONS = ('open', 'openat', 'creat')
CLOSE_FUNCTIONS = ('close',)

# Function name fragments that represent an actual data-transfer call and
# therefore belong in df_intervals (as opposed to metadata-only calls like
# open/close/lseek/fsync, which only ever land in df_posix_records)
DATA_FUNCTIONS = ('read', 'write', 'pread', 'pwrite', 'readv', 'writev')


def _is_open(function_name):
    return any(f in function_name for f in OPEN_FUNCTIONS)


def _is_close(function_name):
    return any(f in function_name for f in CLOSE_FUNCTIONS)


def _is_data_transfer(function_name):
    return any(f in function_name for f in DATA_FUNCTIONS)


def load_events(pfw_path):
    """
    Read the .pfw file and return a list of parsed event dicts, sorted by
    timestamp.

    Robustness matters here because eBPF copies FIXED-SIZE argument
    buffers out of the kernel: a string argument shorter than the buffer
    (the path on open/openat is the usual one) leaves whatever was in the
    remaining bytes, which is frequently neither valid UTF-8 nor legal
    inside a JSON string. Three consequences, all handled below:

      - invalid UTF-8 -> decode per line with errors='replace' instead of
        letting one bad byte abort the whole parse
      - NUL and other control bytes inside JSON strings -> json.loads with
        strict=False, which permits them
      - the events may be wrapped in a JSON array (a bare '[' / ']' line
        and a trailing comma per event) depending on the writer

    Lines that still fail are counted and reported rather than silently
    dropped, so a systematically malformed trace cannot masquerade as an
    application with little I/O.
    """
    events = []
    stats = {'lines': 0, 'replaced_bytes': 0, 'malformed': 0}

    with open(pfw_path, 'rb') as f:
        for raw in f:
            stats['lines'] += 1

            try:
                line = raw.decode('utf-8')
            except UnicodeDecodeError:
                line = raw.decode('utf-8', errors='replace')
                stats['replaced_bytes'] += 1

            line = line.strip()
            if not line or line in ('[', ']'):
                continue
            if line.endswith(','):
                line = line[:-1]

            try:
                # strict=False allows the literal control characters that
                # uninitialised buffer padding leaves inside string values
                event = json.loads(line, strict=False)
            except json.JSONDecodeError:
                stats['malformed'] += 1
                continue

            if not isinstance(event, dict):
                stats['malformed'] += 1
                continue

            if event.get('ph') != 'X':
                continue

            if 'ts' not in event:
                stats['malformed'] += 1
                continue

            events.append(event)

    if stats['replaced_bytes']:
        print('Note: {} of {} lines contained bytes that are not valid UTF-8 (typical of fixed-size eBPF '
              'argument buffers); they were decoded with replacement.'.format(
                  stats['replaced_bytes'], stats['lines']))

    if stats['malformed']:
        print('Warning: {} of {} lines could not be parsed as trace events and were skipped.'.format(
            stats['malformed'], stats['lines']))

    if not events:
        print('Warning: no complete (ph="X") events were parsed from {}.'.format(pfw_path))

    events.sort(key=lambda e: e['ts'])
    return events


def clean_path(value):
    """
    Recover the path from a fixed-size buffer argument. The kernel copies
    the whole buffer, so the C string ends at the first NUL and anything
    after it is leftover memory, not part of the filename.
    """
    if not isinstance(value, str):
        return None

    path = value.split('\x00', 1)[0].strip()

    # A buffer that filled to capacity has no NUL to cut at, so any
    # undecodable tail survives as replacement characters. Strip those
    # from the end only -- an interior one may be a genuinely odd path,
    # and trimming it would merge two distinct files into one entry.
    path = path.rstrip('\ufffd').strip()

    return path or None


def resolve_file_descriptors(events):
    """
    Walk events in timestamp order and build a per-event resolved filename.

    Returns:
        events: the same list, with a 'resolved_fname' key added to every
                 event whose args carry an 'fd' (None if it can't be
                 resolved, e.g. the fd was opened before the trace started)
        file_map: dict of {file_id: filename} for every filename observed,
                   in the same shape Drishti's other handlers use
    """
    # (pid, fd) -> filename, valid until the matching close()
    open_fds = {}

    # filename -> file_id, and the reverse map Drishti expects (file_map)
    filename_to_id = {}
    file_map = {}

    def get_file_id(filename):
        if filename not in filename_to_id:
            new_id = len(filename_to_id)
            filename_to_id[filename] = new_id
            file_map[new_id] = filename
        return filename_to_id[filename]

    unresolved_fd_count = 0

    for event in events:
        function = event.get('name', '')
        pid = event.get('pid')
        args = event.get('args', {}) or {}
        fd = args.get('fd')

        event['resolved_fname'] = None
        event['file_id'] = None

        if _is_open(function):
            # The path Datacrumbs captured for the syscall argument buffer.
            # It is a fixed-size copy, so trim it back to the C string.
            path = clean_path(args.get('buffer'))
            if fd is not None and path:
                open_fds[(pid, fd)] = path
                event['resolved_fname'] = path
                event['file_id'] = get_file_id(path)
            continue

        if _is_close(function):
            if fd is not None:
                open_fds.pop((pid, fd), None)
            continue

        # Any other event that references a fd (read/write/pread/etc.)
        if fd is not None:
            path = open_fds.get((pid, fd))
            if path is not None:
                event['resolved_fname'] = path
                event['file_id'] = get_file_id(path)
            else:
                # fd was opened before trace capture began -- fall back to a
                # per-(pid, fd) placeholder so the row is still counted
                # instead of silently dropped, but flagged as unresolved.
                placeholder = 'UNRESOLVED_FD::pid{}::fd{}'.format(pid, fd)
                event['resolved_fname'] = placeholder
                event['file_id'] = get_file_id(placeholder)
                unresolved_fd_count += 1

    return events, file_map, unresolved_fd_count


def build_rank_map(events):
    """
    Map each unique pid to a 0-indexed rank, the same convention
    Darshan/Recorder traces use, so the shared rank-based imbalance checks
    don't need any special-casing for eBPF input.
    """
    pids = sorted({e['pid'] for e in events})
    return {pid: rank for rank, pid in enumerate(pids)}


def classify_api(category):
    if category == CAT_MPIIO:
        return 'MPI-IO'
    if category == CAT_POSIX:
        return 'POSIX'
    return None


def build_dataframes(pfw_path):
    """
    Parse a .pfw file into the DataFrames the eBPF handler consumes:

        df_intervals:     one row per data-transfer call (read/write/etc.)
                           columns: file_id, rank, tid, api, function,
                                    start, end, duration, size, offset
        df_posix_records:  one row per POSIX-category call, data-transfer
                           calls AND metadata calls (open/close/lseek/...)
                           columns: fname, rank, tid, function, start,
                                    end, duration
                           (tid is needed to nest syscalls inside the
                           library-layer intervals in df_layers)
        df_layers:         one row per NON-syscall event (library layers:
                           HDF5 internals, VOL, MPI, ...) -- the enclosing
                           intervals used to attribute syscalls to the
                           software layer that issued them
                           columns: rank, tid, cat, function, start, end,
                                    duration
        file_map: {file_id: filename}

    Returns (df_intervals, df_posix_records, df_layers, file_map)
    """
    events = load_events(pfw_path)
    events, file_map, unresolved_fd_count = resolve_file_descriptors(events)
    rank_map = build_rank_map(events)

    if unresolved_fd_count:
        print(
            'Warning: {} events referenced a file descriptor opened before '
            'trace capture began; grouped under placeholder file '
            'entries.'.format(unresolved_fd_count)
        )

    interval_rows = []
    posix_record_rows = []
    layer_rows = []

    for event in events:
        function = event.get('name', '')
        category = event.get('cat')
        pid = event.get('pid')
        tid = event.get('tid', pid)
        rank = rank_map[pid]
        start = event['ts'] * US_TO_S
        dur = (event.get('dur', 0) or 0) * US_TO_S
        end = start + dur
        args = event.get('args', {}) or {}

        api = classify_api(category)

        # df_posix_records: every POSIX-category call (data transfer and
        # metadata alike), used for the aggregate per-rank time check
        if category == CAT_POSIX:
            posix_record_rows.append([
                event['resolved_fname'], rank, tid, function, start, end, dur
            ])
        else:
            # Everything that is not a raw syscall is a software layer
            # above the syscalls (HDF5 internals, VOL connectors, MPI, ...)
            # whose interval encloses the syscalls it caused. MPI data
            # calls intentionally appear BOTH here and in df_intervals:
            # they are data transfers in their own right and enclosing
            # layers for the POSIX calls beneath them.
            layer_rows.append([
                rank, tid, category, function, start, end, dur
            ])

        # df_intervals: only actual data-transfer calls on a recognized API
        if api is not None and _is_data_transfer(function):
            size = args.get('count', 0) or 0
            offset = args.get('offset', 0) or 0

            interval_rows.append([
                event['file_id'], rank, tid, api, function,
                start, end, dur, size, offset
            ])

    df_intervals = pd.DataFrame(
        interval_rows,
        columns=['file_id', 'rank', 'tid', 'api', 'function',
                 'start', 'end', 'duration', 'size', 'offset']
    )

    df_posix_records = pd.DataFrame(
        posix_record_rows,
        columns=['fname', 'rank', 'tid', 'function', 'start', 'end', 'duration']
    )

    df_layers = pd.DataFrame(
        layer_rows,
        columns=['rank', 'tid', 'cat', 'function', 'start', 'end', 'duration']
    )

    return df_intervals, df_posix_records, df_layers, file_map
