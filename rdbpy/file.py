import os
import struct
from io import BufferedRandom
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Union


@dataclass(frozen=True)
class BlockId:
    filename: str
    blknum: int

    def __str__(self) -> str:
        return f"[file {self.filename}, block {self.blknum}]"


class Page:
    _buffer: bytearray

    def __init__(self, arg: Union[int, bytes]) -> None:
        if isinstance(arg, int):
            self._buffer = bytearray(arg)
        elif isinstance(arg, bytes):
            self._buffer = bytearray(arg)
        else:
            raise ValueError("`arg` must be int or bytes")

    def get_int(self, offset: int) -> int:
        return struct.unpack("<i", self._buffer[offset : offset + 4])[0]

    def set_int(self, offset: int, n: int) -> None:
        self._buffer[offset : offset + 4] = struct.pack("<i", n)

    def get_bytes(self, offset: int) -> bytes:
        length: int = struct.unpack("<I", self._buffer[offset : offset + 4])[0]
        return bytes(self._buffer[offset + 4 : offset + 4 + length])

    def set_bytes(self, offset: int, b: bytes) -> None:
        self._buffer[offset : offset + 4] = struct.pack("<I", len(b))
        self._buffer[offset + 4 : offset + 4 + len(b)] = b

    def get_string(self, offset: int) -> str:
        b = self.get_bytes(offset)
        return b.decode()

    def set_string(self, offset: int, s: str) -> None:
        b = s.encode()
        self.set_bytes(offset, b)

    @staticmethod
    def max_length(strlen: int) -> int:
        return 4 + strlen * 4  # utf-8 の最大バイト数は4

    def contents(self) -> bytearray:
        return self._buffer


class FileMgr:
    _db_directory: Path
    _blocksize: int
    _is_new: bool
    _open_files: dict[str, BufferedRandom]
    _lock: Lock

    def __init__(self, db_directory: Path, blocksize: int) -> None:
        self._db_directory = db_directory
        self._blocksize = blocksize
        self._is_new = not self._db_directory.exists()
        if self._is_new:
            self._db_directory.mkdir(parents=True)

        for filepath in self._db_directory.iterdir():
            if filepath.name.startswith("temp"):
                filepath.unlink()

        self._open_files = {}
        self._lock = Lock()

    def read(self, blk: BlockId, p: Page) -> None:
        with self._lock:
            try:
                f = self._get_file(blk.filename)
                f.seek(blk.blknum * self._blocksize)
                f.readinto(p.contents())
            except Exception as e:
                print(e)
                raise RuntimeError(f"cannot read block {blk}")

    def write(self, blk: BlockId, p: Page) -> None:
        with self._lock:
            try:
                f = self._get_file(blk.filename)
                f.seek(blk.blknum * self._blocksize)
                f.write(p.contents())
            except Exception as e:
                print(e)
                raise RuntimeError(f"cannot write block {blk}")

    def append(self, filename: str) -> BlockId:
        with self._lock:
            newblknum = self.length(filename)
            blk = BlockId(filename, newblknum)
            b = bytes(self._blocksize)
            try:
                f = self._get_file(blk.filename)
                f.seek(blk.blknum * self._blocksize)
                f.write(b)
            except Exception as e:
                print(e)
                raise RuntimeError(f"cannot append block {blk}")
            return blk

    def length(self, filename: str) -> int:
        try:
            f = self._get_file(filename)
            f.seek(0, os.SEEK_END)
            f_len = f.tell()
            assert f_len % self._blocksize == 0
            return f_len // self._blocksize
        except Exception as e:
            print(e)
            raise RuntimeError(f"cannot access {filename}")

    @property
    def is_new(self) -> bool:
        return self._is_new

    @property
    def block_size(self) -> int:
        return self._blocksize

    def _get_file(self, filename: str):
        f = self._open_files.get(filename)
        if f is None:
            db_table = self._db_directory / filename
            if not db_table.exists():
                db_table.touch()
            f = open(db_table, "rb+")
            self._open_files[filename] = f
        return f

    def __del__(self) -> None:
        for f in self._open_files.values():
            f.close()


"""
BlockId: filename の何ブロック目の Page を読み書きするか指定する
Page: int, str, bytes をバイト列でシリアライズしてインメモリに保持する
FileMgr: BlockId, Page を受け取り、ファイルの読み書きをする
"""
