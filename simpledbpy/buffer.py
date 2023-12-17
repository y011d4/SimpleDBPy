import threading
import time
from typing import Optional, Sequence

from simpledbpy.file import BlockId, FileMgr, Page
from simpledbpy.log import LogMgr


class BufferAbortError(RuntimeError):
    pass


class Buffer:
    _fm: FileMgr
    _lm: LogMgr
    _contents: Page
    _blk: Optional[BlockId]
    _pins: int
    _txnum = -1
    _lsn = -1

    def __init__(self, fm: FileMgr, lm: LogMgr) -> None:
        self._fm = fm
        self._lm = lm
        self._contents = Page(self._fm.block_size)
        self._blk = None
        self._pins = 0
        self._txnum = -1
        self._lsn = -1

    @property
    def contents(self) -> Page:
        return self._contents

    @property
    def block(self) -> Optional[BlockId]:
        return self._blk

    def set_modified(self, txnum: int, lsn: int):
        self._txnum = txnum
        if lsn >= 0:
            self._lsn = lsn

    def is_pinned(self) -> bool:
        return self._pins > 0

    @property
    def modifying_tx(self) -> int:
        return self._txnum

    def assign_to_block(self, b: BlockId) -> None:
        self.flush()
        self._blk = b
        self._fm.read(self._blk, self._contents)
        self._pins = 0

    def flush(self) -> None:
        if self._txnum >= 0 and self._blk is not None:
            self._lm.flush(self._lsn)
            self._fm.write(self._blk, self._contents)
            self._txnum -= 1

    def pin(self) -> None:
        self._pins += 1

    def unpin(self) -> None:
        self._pins -= 1


class BufferMgr:
    MAX_TIME: int = 10

    _bufferpool: Sequence[Buffer]
    _num_available: int
    _cv: threading.Condition

    def __init__(self, fm: FileMgr, lm: LogMgr, numbuffs: int) -> None:
        self._bufferpool = [Buffer(fm, lm) for _ in range(numbuffs)]
        self._num_available = numbuffs
        self._cv = threading.Condition()

    @property
    def available(self) -> int:
        with self._cv:
            return self._num_available

    def flush_all(self, txnum: int) -> None:
        with self._cv:
            for buff in self._bufferpool:
                if buff.modifying_tx == txnum:
                    buff.flush()

    def unpin(self, buff: Buffer) -> None:
        with self._cv:
            buff.unpin()
            if not buff.is_pinned():
                self._num_available += 1
                self._cv.notify_all()

    def pin(self, blk: BlockId) -> Buffer:
        with self._cv:
            timestamp = int(time.time())
            buff = self._try_to_pin(blk)
            while buff is None and not self._waiting_too_long(timestamp):
                self._cv.wait(self.MAX_TIME)
                buff = self._try_to_pin(blk)
            if buff is None:
                raise BufferAbortError()
            return buff

    def _waiting_too_long(self, starttime: int) -> bool:
        return time.time() - starttime > self.MAX_TIME

    def _try_to_pin(self, blk: BlockId) -> Optional[Buffer]:
        buff = self._find_existing_buffer(blk)
        if buff is None:
            buff = self._choose_unpinned_buffer()
            if buff is None:
                return None
            buff.assign_to_block(blk)
        if not buff.is_pinned():
            self._num_available -= 1
        buff.pin()
        return buff

    def _find_existing_buffer(self, blk: BlockId) -> Optional[Buffer]:
        for buff in self._bufferpool:
            b = buff.block
            if b is not None and b == blk:
                return buff
        return None

    def _choose_unpinned_buffer(self) -> Optional[Buffer]:
        # TODO: LRU などに置き換える
        for buff in self._bufferpool:
            if not buff.is_pinned():
                return buff
        return None
