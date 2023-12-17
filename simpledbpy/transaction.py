from threading import Lock
from simpledbpy.buffer import Buffer, BufferMgr
from simpledbpy.concurrency import ConcurrencyMgr
from simpledbpy.file import BlockId, FileMgr
from simpledbpy.log import LogMgr
from simpledbpy.recovery import RecoveryMgr


class BufferList:
    _buffers: dict[BlockId, Buffer]
    _pins: list[BlockId]
    _bm: BufferMgr

    def __init__(self, bm: BufferMgr) -> None:
        self._buffers = {}
        self._pins = []
        self._bm = bm

    def get_buffer(self, blk: BlockId) -> Buffer:
        ret = self._buffers.get(blk)
        if ret is None:
            raise RuntimeError
        return ret

    def pin(self, blk: BlockId) -> None:
        buff = self._bm.pin(blk)
        self._buffers[blk] = buff
        self._pins.append(blk)

    def unpin(self, blk: BlockId) -> None:
        buff = self._buffers.get(blk)
        if buff is None:
            raise RuntimeError
        self._bm.unpin(buff)
        self._pins.remove(blk)
        if blk not in self._pins:
            del self._buffers[blk]

    def unpin_all(self) -> None:
        for blk in self._pins:
            buff = self._buffers.get(blk)
            assert buff is not None
            self._bm.unpin(buff)
        self._buffers.clear()
        self._pins.clear()


class Transaction:
    END_OF_FILE = -1

    _next_tx_num = 0
    _lock = Lock()

    _bm: BufferMgr
    _fm: FileMgr
    _recovery_mgr: RecoveryMgr
    _concur_mgr: ConcurrencyMgr
    _txnum: int
    _mybuffers: BufferList

    def __init__(self, fm: FileMgr, lm: LogMgr, bm: BufferMgr) -> None:
        self._bm = bm
        self._fm = fm
        self._txnum = self._next_tx_number()
        self._recovery_mgr = RecoveryMgr(self, self._txnum, lm, bm)
        self._concur_mgr = ConcurrencyMgr()
        self._mybuffers = BufferList(bm)

    def commit(self) -> None:
        self._recovery_mgr.commit()
        self._concur_mgr.release()
        self._mybuffers.unpin_all()
        print(f"transaction {self._txnum} commited")

    def rollback(self) -> None:
        self._recovery_mgr.rollback()
        self._concur_mgr.release()
        self._mybuffers.unpin_all()
        print(f"transaction {self._txnum} rolled back")

    def recover(self) -> None:
        self._bm.flush_all(self._txnum)
        self._recovery_mgr.recover()

    def pin(self, blk: BlockId) -> None:
        self._mybuffers.pin(blk)

    def unpin(self, blk: BlockId) -> None:
        self._mybuffers.unpin(blk)

    def get_int(self, blk: BlockId, offset: int) -> int:
        self._concur_mgr.slock(blk)
        buff = self._mybuffers.get_buffer(blk)
        return buff.contents.get_int(offset)

    def get_string(self, blk: BlockId, offset: int) -> str:
        self._concur_mgr.slock(blk)
        buff = self._mybuffers.get_buffer(blk)
        return buff.contents.get_string(offset)

    def set_int(self, blk: BlockId, offset: int, val: int, ok_to_log: bool) -> None:
        self._concur_mgr.xlock(blk)
        buff = self._mybuffers.get_buffer(blk)
        lsn = -1
        if ok_to_log:
            lsn = self._recovery_mgr.set_int(buff, offset, val)
        p = buff.contents
        p.set_int(offset, val)
        buff.set_modified(self._txnum, lsn)

    def set_string(self, blk: BlockId, offset: int, val: str, ok_to_log: bool) -> None:
        self._concur_mgr.xlock(blk)
        buff = self._mybuffers.get_buffer(blk)
        lsn = -1
        if ok_to_log:
            lsn = self._recovery_mgr.set_string(buff, offset, val)
        p = buff.contents
        p.set_string(offset, val)
        buff.set_modified(self._txnum, lsn)

    def size(self, filename: str) -> int:
        dummyblk = BlockId(filename, Transaction.END_OF_FILE)
        self._concur_mgr.slock(dummyblk)
        return self._fm.length(filename)

    def append(self, filename: str) -> BlockId:
        dummyblk = BlockId(filename, Transaction.END_OF_FILE)
        self._concur_mgr.xlock(dummyblk)
        return self._fm.append(filename)

    def block_size(self) -> int:
        return self._fm.block_size

    def available_buffs(self) -> int:
        return self._bm.available

    def _next_tx_number(self) -> int:
        with Transaction._lock:
            Transaction._next_tx_num += 1
            print(f"new transaction: {Transaction._next_tx_num}")
            return Transaction._next_tx_num


"""
class Transaction:
    END_OF_FILE = -1
    _next_tx_num: int = 0

    def __init__(self):
        self._txnum = Transaction._next_tx_number()
        print(self._txnum)

    @classmethod
    def _next_tx_number(cls) -> int:
        cls._next_tx_num += 1
        return cls._next_tx_num


class Transaction:
    END_OF_FILE = -1
    _next_tx_num: int = 0
    hoge = Lock()

    def __init__(self):
        self._txnum = self._next_tx_number()
        print(self._txnum)
        print(self.hoge)

    def _next_tx_number(self) -> int:
        Transaction._next_tx_num += 1
        return Transaction._next_tx_num
"""
