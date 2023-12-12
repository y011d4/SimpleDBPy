from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("rdbpy")
except PackageNotFoundError:
    __version__ = "0.dev0"
