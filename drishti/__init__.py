from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("drishti-io")
except PackageNotFoundError:
    __version__ = "unknown"