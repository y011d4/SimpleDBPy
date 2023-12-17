import time
from threading import Condition

from rdbpy.file import BlockId


class LockAbortError(RuntimeError):
    pass


class LockTable:
    MAX_TIME = 10

    _locks: dict[BlockId, int]
    _cv: Condition

    def __init__(self) -> None:
        self._locks = {}
        self._cv = Condition()

    def slock(self, blk: BlockId) -> None:
        with self._cv:
            try:
                timestamp = int(time.time())
                while self._has_xlock(blk) and not self._waiting_too_long(timestamp):
                    self._cv.wait(self.MAX_TIME)
                if self._has_xlock(blk):
                    raise LockAbortError
                val = self._get_lock_val(blk)
                self._locks[blk] = val + 1
            except InterruptedError:
                raise LockAbortError

    def xlock(self, blk: BlockId) -> None:
        with self._cv:
            try:
                timestamp = int(time.time())
                while self._has_other_slocks(blk) and not self._waiting_too_long(
                    timestamp
                ):
                    self._cv.wait(self.MAX_TIME)
                if self._has_other_slocks(blk):
                    raise LockAbortError
                self._locks[blk] = -1
            except InterruptedError:
                raise LockAbortError

    def unlock(self, blk: BlockId) -> None:
        with self._cv:
            val = self._get_lock_val(blk)
            if blk not in self._locks:
                raise RuntimeError
            if val > 1:
                self._locks[blk] = val - 1
            else:
                del self._locks[blk]
                self._cv.notify_all()

    def _has_xlock(self, blk: BlockId) -> bool:
        return self._get_lock_val(blk) < 0

    def _has_other_slocks(self, blk: BlockId) -> bool:
        # xlock を取る前に slock を取るように使うため、他の transaction が slock を取っているかは >1 で判定する
        return self._get_lock_val(blk) > 1

    def _waiting_too_long(self, starttime: int) -> bool:
        return time.time() - starttime > self.MAX_TIME

    def _get_lock_val(self, blk: BlockId) -> int:
        return self._locks.get(blk, 0)


class ConcurrencyMgr:
    _locktbl = LockTable()

    _locks: dict[BlockId, str]

    def __init__(self) -> None:
        self._locks = {}

    def slock(self, blk: BlockId) -> None:
        if blk not in self._locks:
            self._locktbl.slock(blk)
            self._locks[blk] = "S"

    def xlock(self, blk: BlockId) -> None:
        if not self._has_xlock(blk):
            self.slock(blk)
            self._locktbl.xlock(blk)
            self._locks[blk] = "X"

    def release(self) -> None:
        for blk in self._locks:
            self._locktbl.unlock(blk)
        self._locks.clear()

    def _has_xlock(self, blk: BlockId) -> bool:
        locktype = self._locks.get(blk)
        return locktype is not None and locktype == "X"
