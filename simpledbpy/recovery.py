from __future__ import annotations
from abc import abstractmethod
from enum import Enum
from typing import Optional, TYPE_CHECKING

from simpledbpy.buffer import Buffer, BufferMgr
from simpledbpy.file import BlockId, Page
from simpledbpy.log import LogMgr

if TYPE_CHECKING:
    from simpledbpy.transaction import Transaction


class LogType(Enum):
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5


class LogRecord:
    @abstractmethod
    def op(self) -> LogType:
        raise NotImplementedError

    @abstractmethod
    def tx_number(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def undo(self, tx: Transaction) -> None:
        raise NotImplementedError

    @staticmethod
    def create_log_record(b: bytes) -> Optional["LogRecord"]:
        p = Page(b)
        log_type = LogType(p.get_int(0))
        if log_type == LogType.CHECKPOINT:
            return CheckpointRecord()
        elif log_type == LogType.START:
            return StartRecord(p)
        elif log_type == LogType.COMMIT:
            return CommitRecord(p)
        elif log_type == LogType.ROLLBACK:
            return RollbackRecord(p)
        elif log_type == LogType.SETINT:
            return SetIntRecord(p)
        elif log_type == LogType.SETSTRING:
            return SetStringRecord(p)
        else:
            return None


class CheckpointRecord(LogRecord):
    def op(self) -> LogType:
        return LogType.CHECKPOINT

    def tx_number(self) -> int:
        return -1

    def __str__(self) -> str:
        return "<CHECKPOINT>"

    def undo(self, _tx: Transaction) -> None:
        pass

    @staticmethod
    def write_to_log(lm: LogMgr) -> int:
        reclen = 4
        p = Page(bytes(reclen))
        p.set_int(0, LogType.CHECKPOINT.value)
        return lm.append(p.contents())


class StartRecord(LogRecord):
    _txnum: int

    def __init__(self, p: Page) -> None:
        tpos = 4
        self._txnum = p.get_int(tpos)

    def op(self) -> LogType:
        return LogType.START

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<START {self._txnum}>"

    def undo(self, _tx: Transaction) -> None:
        pass

    @staticmethod
    def write_to_log(lm: LogMgr, txnum: int) -> int:
        tpos = 4
        reclen = tpos + 4
        p = Page(bytes(reclen))
        p.set_int(0, LogType.START.value)
        p.set_int(tpos, txnum)
        return lm.append(p.contents())


class CommitRecord(LogRecord):
    _txnum: int

    def __init__(self, p: Page) -> None:
        tpos = 4
        self._txnum = p.get_int(tpos)

    def op(self) -> LogType:
        return LogType.COMMIT

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<COMMIT {self._txnum}>"

    def undo(self, _tx: Transaction) -> None:
        pass

    @staticmethod
    def write_to_log(lm: LogMgr, txnum: int) -> int:
        tpos = 4
        reclen = tpos + 4
        p = Page(bytes(reclen))
        p.set_int(0, LogType.COMMIT.value)
        p.set_int(tpos, txnum)
        return lm.append(p.contents())


class RollbackRecord(LogRecord):
    _txnum: int

    def __init__(self, p: Page) -> None:
        tpos = 4
        self._txnum = p.get_int(tpos)

    def op(self) -> LogType:
        return LogType.ROLLBACK

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<ROLLBACK {self._txnum}>"

    def undo(self, _tx: Transaction) -> None:
        pass

    @staticmethod
    def write_to_log(lm: LogMgr, txnum: int) -> int:
        tpos = 4
        reclen = tpos + 4
        p = Page(bytes(reclen))
        p.set_int(0, LogType.ROLLBACK.value)
        p.set_int(tpos, txnum)
        return lm.append(p.contents())


class SetIntRecord(LogRecord):
    _txnum: int
    _offset: int
    _val: int
    _blk: BlockId

    def __init__(self, p: Page) -> None:
        tpos = 4
        self._txnum = p.get_int(tpos)
        fpos = tpos + 4
        filename = p.get_string(fpos)
        bpos = fpos + Page.max_length(len(filename))
        blknum = p.get_int(bpos)
        self._blk = BlockId(filename, blknum)
        opos = bpos + 4
        self._offset = p.get_int(opos)
        vpos = opos + 4
        self._val = p.get_int(vpos)

    def op(self) -> LogType:
        return LogType.SETINT

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<SETINT {self._txnum} {self._blk} {self._offset} {self._val}>"

    def undo(self, tx: Transaction) -> None:
        tx.pin(self._blk)
        tx.set_int(self._blk, self._offset, self._val, False)
        tx.unpin(self._blk)

    @staticmethod
    def write_to_log(
        lm: LogMgr, txnum: int, blk: BlockId, offset: int, val: int
    ) -> int:
        tpos = 4
        fpos = tpos + 4
        bpos = fpos + Page.max_length(len(blk.filename))
        opos = bpos + 4
        vpos = opos + 4
        reclen = vpos + 4
        p = Page(bytes(reclen))
        p.set_int(0, LogType.SETINT.value)
        p.set_int(tpos, txnum)
        p.set_string(fpos, blk.filename)
        p.set_int(bpos, blk.blknum)
        p.set_int(opos, offset)
        p.set_int(vpos, val)
        return lm.append(p.contents())


class SetStringRecord(LogRecord):
    _txnum: int
    _offset: int
    _val: str
    _blk: BlockId

    def __init__(self, p: Page) -> None:
        tpos = 4
        self._txnum = p.get_int(tpos)
        fpos = tpos + 4
        filename = p.get_string(fpos)
        bpos = fpos + Page.max_length(len(filename))
        blknum = p.get_int(bpos)
        self._blk = BlockId(filename, blknum)
        opos = bpos + 4
        self._offset = p.get_int(opos)
        vpos = opos + 4
        self._val = p.get_string(vpos)

    def op(self) -> LogType:
        return LogType.SETSTRING

    def tx_number(self) -> int:
        return self._txnum

    def __str__(self) -> str:
        return f"<SETSTRING {self._txnum} {self._blk} {self._offset} {self._val}>"

    def undo(self, tx: Transaction) -> None:
        tx.pin(self._blk)
        tx.set_string(self._blk, self._offset, self._val, False)
        tx.unpin(self._blk)

    @staticmethod
    def write_to_log(
        lm: LogMgr, txnum: int, blk: BlockId, offset: int, val: str
    ) -> int:
        tpos = 4
        fpos = tpos + 4
        bpos = fpos + Page.max_length(len(blk.filename))
        opos = bpos + 4
        vpos = opos + 4
        reclen = vpos + Page.max_length(len(val))
        p = Page(bytes(reclen))
        p.set_int(0, LogType.SETSTRING.value)
        p.set_int(tpos, txnum)
        p.set_string(fpos, blk.filename)
        p.set_int(bpos, blk.blknum)
        p.set_int(opos, offset)
        p.set_string(vpos, val)
        return lm.append(p.contents())


class RecoveryMgr:
    _lm: LogMgr
    _bm: BufferMgr
    _tx: Transaction
    _txnum: int

    def __init__(self, tx: Transaction, txnum: int, lm: LogMgr, bm: BufferMgr) -> None:
        self._tx = tx
        self._txnum = txnum
        self._lm = lm
        self._bm = bm
        StartRecord.write_to_log(self._lm, self._txnum)

    def commit(self) -> None:
        self._bm.flush_all(self._txnum)
        lsn = CommitRecord.write_to_log(self._lm, self._txnum)
        self._lm.flush(lsn)

    def rollback(self) -> None:
        self._do_rollback()
        self._bm.flush_all(self._txnum)
        lsn = RollbackRecord.write_to_log(self._lm, self._txnum)
        self._lm.flush(lsn)

    def recover(self) -> None:
        self._do_recover()
        self._bm.flush_all(self._txnum)
        lsn = CheckpointRecord.write_to_log(self._lm)
        self._lm.flush(lsn)

    def set_int(self, buff: Buffer, offset: int, newval: int) -> int:
        oldval = buff.contents.get_int(offset)
        blk = buff.block
        assert blk is not None
        return SetIntRecord.write_to_log(self._lm, self._txnum, blk, offset, oldval)

    def set_string(self, buff: Buffer, offset: int, newval: str) -> int:
        oldval = buff.contents.get_string(offset)
        blk = buff.block
        assert blk is not None
        return SetStringRecord.write_to_log(self._lm, self._txnum, blk, offset, oldval)

    def _do_rollback(self) -> None:
        for b in self._lm:
            rec = LogRecord.create_log_record(b)
            assert rec is not None
            if rec.tx_number() == self._txnum:
                if rec.op() == LogType.START:
                    return
                rec.undo(self._tx)

    def _do_recover(self) -> None:
        finished_txs = []
        for b in self._lm:
            rec = LogRecord.create_log_record(b)
            assert rec is not None
            if rec.op() == LogType.CHECKPOINT:
                return
            if rec.op() == LogType.COMMIT or rec.op() == LogType.ROLLBACK:
                finished_txs.append(rec.tx_number())
            elif rec.tx_number() not in finished_txs:
                rec.undo(self._tx)
