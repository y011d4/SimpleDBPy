from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("simpledbpy")
except PackageNotFoundError:
    __version__ = "0.dev0"
