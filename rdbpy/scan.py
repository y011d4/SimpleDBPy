from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional
from rdbpy.file import BlockId
from rdbpy.record import Layout, RecordPage, Types

from rdbpy.transaction import Transaction


@dataclass
class Constant:
    ival: Optional[int]
    sval: Optional[str]

    @classmethod
    def from_int(cls, ival: int) -> "Constant":
        return Constant(ival=ival, sval=None)

    @classmethod
    def from_string(cls, sval: str) -> "Constant":
        return Constant(ival=None, sval=sval)


@dataclass
class RID:
    blknum: int
    slot: int

    def __str__(self) -> str:
        return f"[{self.blknum}, {self.slot}]"


class Scan:
    @abstractmethod
    def before_first(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def next(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_int(self, fldname: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_string(self, fldname: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_val(self, fldname: str) -> Constant:
        raise NotImplementedError

    @abstractmethod
    def has_field(self, fldname: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class UpdateScan(Scan):
    @abstractmethod
    def set_val(self, fldname: str, val: Constant) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_int(self, fldname: str, val: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_string(self, fldname: str, val: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def insert(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_rid(self) -> RID:
        raise NotImplementedError

    @abstractmethod
    def move_to_rid(self, rid: RID) -> None:
        raise NotImplementedError


class TableScan(UpdateScan):
    _tx: Transaction
    _layout: Layout
    _rp: Optional[RecordPage]
    _filename: str
    _currentslot: int

    def __init__(self, tx: Transaction, tblname: str, layout: Layout) -> None:
        self._tx = tx
        self._layout = layout
        self._filename = f"{tblname}.tbl"
        self._rp = None
        if self._tx.size(self._filename) == 0:
            self._move_to_new_block()
        else:
            self._move_to_block(0)

    def close(self) -> None:
        if self._rp is not None:
            self._tx.unpin(self._rp.block())

    def before_first(self) -> None:
        self._move_to_block(0)

    def next(self) -> bool:
        assert self._rp is not None
        self._currentslot = self._rp.next_after(self._currentslot)
        while self._currentslot < 0:
            if self._at_last_block():
                return False
            currentblk = self._rp.block()
            self._move_to_block(currentblk.blknum + 1)
            self._currentslot = self._rp.next_after(self._currentslot)
        return True

    def get_int(self, fldname: str) -> int:
        assert self._rp is not None
        return self._rp.get_int(self._currentslot, fldname)

    def get_string(self, fldname: str) -> str:
        assert self._rp is not None
        return self._rp.get_string(self._currentslot, fldname)

    def get_val(self, fldname: str) -> Constant:
        fldtype = self._layout.schema().type(fldname)
        if fldtype == Types.INTEGER:
            return Constant.from_int(self.get_int(fldname))
        elif fldtype == Types.VARCHAR:
            return Constant.from_string(self.get_string(fldname))
        else:
            raise ValueError("Please modify scan.py")

    def has_field(self, fldname: str) -> bool:
        return self._layout.schema().has_field(fldname)

    def set_int(self, fldname: str, val: int) -> None:
        assert self._rp is not None
        self._rp.set_int(self._currentslot, fldname, val)

    def set_string(self, fldname: str, val: str) -> None:
        assert self._rp is not None
        self._rp.set_string(self._currentslot, fldname, val)

    def set_val(self, fldname: str, val: Constant) -> None:
        fldtype = self._layout.schema().type(fldname)
        if fldtype == Types.INTEGER:
            assert val.ival is not None
            self.set_int(fldname, val.ival)
        elif fldtype == Types.VARCHAR:
            assert val.sval is not None
            self.set_string(fldname, val.sval)
        else:
            raise ValueError("Please modify scan.py")

    def insert(self) -> None:
        assert self._rp is not None
        self._currentslot = self._rp.insert_after(self._currentslot)
        while self._currentslot < 0:
            if self._at_last_block():
                self._move_to_new_block()
            else:
                self._move_to_block(self._rp.block().blknum + 1)
            self._currentslot = self._rp.insert_after(self._currentslot)

    def delete(self) -> None:
        assert self._rp is not None
        self._rp.delete(self._currentslot)

    def move_to_rid(self, rid: RID) -> None:
        self.close()
        blk = BlockId(self._filename, rid.blknum)
        self._rp = RecordPage(self._tx, blk, self._layout)
        self._currentslot = rid.slot

    def get_rid(self) -> RID:
        assert self._rp is not None
        return RID(blknum=self._rp.block().blknum, slot=self._currentslot)

    def _move_to_block(self, blknum: int) -> None:
        self.close()
        blk = BlockId(self._filename, blknum)
        self._rp = RecordPage(self._tx, blk, self._layout)
        self._currentslot = -1

    def _move_to_new_block(self) -> None:
        self.close()
        blk = self._tx.append(self._filename)
        self._rp = RecordPage(self._tx, blk, self._layout)
        self._rp.format()
        self._currentslot = -1

    def _at_last_block(self) -> bool:
        assert self._rp is not None
        return self._rp.block().blknum == self._tx.size(self._filename) - 1
