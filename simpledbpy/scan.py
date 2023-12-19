from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, Sequence

from simpledbpy.file import BlockId
from simpledbpy.grammar import Constant, Term
from simpledbpy.record import Layout, RecordPage, Schema, Types
from simpledbpy.transaction import Transaction


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


class Predicate:
    _terms: list[Term]

    def __init__(self, t: Optional[Sequence[Term]] = None) -> None:
        if t is None:
            self._terms = []
        else:
            self._terms = list(t)

    def conjoin_with(self, pred: "Predicate") -> None:
        self._terms += pred._terms

    def is_satisfied(self, s: Scan) -> bool:
        return all([t.is_satisfied(s) for t in self._terms])

    # def reduction_factor(self, p: Plan) -> int:
    #     pass

    def select_sub_pred(self, sch: Schema) -> Optional["Predicate"]:
        newterms = list(filter(lambda t: t.applies_to(sch), self._terms))
        return None if len(newterms) == 0 else Predicate(newterms)

    def join_sub_pred(self, sch1: Schema, sch2: Schema) -> Optional["Predicate"]:
        newsch = Schema()
        newsch.add_all(sch1)
        newsch.add_all(sch2)
        # NOTE: 例えば sch1: A1, B1, sch2: A2, B2 という field を持つとき、 A1=B1, A1=B2 などは newterms となるが、 A1=B1 は newterms とならない
        newterms = list(
            filter(
                lambda t: not t.applies_to(sch1)
                and not t.applies_to(sch2)
                and t.applies_to(newsch),
                self._terms,
            )
        )
        return None if len(newterms) == 0 else Predicate(newterms)

    # def equates_with_constant(self, fldname: str) -> Constant:
    #     pass

    # def equates_with_field(self, fldname: str) -> str:
    #     pass

    def __str__(self) -> str:
        return " and ".join([str(t) for t in self._terms])

    def __repr__(self) -> str:
        return self.__str__()


class SelectScan(UpdateScan):
    _s: Scan
    _pred: Predicate

    def __init__(self, s: Scan, pred: Predicate) -> None:
        self._s = s
        self._pred = pred

    def before_first(self) -> None:
        self._s.before_first()

    def next(self) -> bool:
        while self._s.next():
            if self._pred.is_satisfied(self._s):
                return True
        return False

    def get_int(self, fldname: str) -> int:
        return self._s.get_int(fldname)

    def get_string(self, fldname: str) -> str:
        return self._s.get_string(fldname)

    def get_val(self, fldname: str) -> Constant:
        return self._s.get_val(fldname)

    def has_field(self, fldname: str) -> bool:
        return self._s.has_field(fldname)

    def close(self) -> None:
        self._s.close()

    def set_int(self, fldname: str, val: int) -> None:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        self._s.set_int(fldname, val)

    def set_string(self, fldname: str, val: str) -> None:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        self._s.set_string(fldname, val)

    def set_val(self, fldname: str, val: Constant) -> None:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        self._s.set_val(fldname, val)

    def delete(self) -> None:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        self._s.delete()

    def insert(self) -> None:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        self._s.insert()

    def get_rid(self) -> RID:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        return self._s.get_rid()

    def move_to_rid(self, rid: RID) -> None:
        if not isinstance(self._s, UpdateScan):
            raise RuntimeError
        self._s.move_to_rid(rid)


class ProjectScan(Scan):
    _s: Scan
    _fieldlist: Sequence[str]

    def __init__(self, s: Scan, fieldlist: Sequence[str]) -> None:
        self._s = s
        self._fieldlist = fieldlist

    def before_first(self) -> None:
        self._s.before_first()

    def next(self) -> bool:
        return self._s.next()

    def get_int(self, fldname: str) -> int:
        if self.has_field(fldname):
            return self._s.get_int(fldname)
        else:
            raise RuntimeError(f"field {fldname} not found")

    def get_string(self, fldname: str) -> str:
        if self.has_field(fldname):
            return self._s.get_string(fldname)
        else:
            raise RuntimeError(f"field {fldname} not found")

    def get_val(self, fldname: str) -> Constant:
        if self.has_field(fldname):
            return self._s.get_val(fldname)
        else:
            raise RuntimeError(f"field {fldname} not found")

    def has_field(self, fldname: str) -> bool:
        return fldname in self._fieldlist

    def close(self) -> None:
        self._s.close()


class ProductScan(Scan):
    _s1: Scan
    _s2: Scan

    def __init__(self, s1: Scan, s2: Scan) -> None:
        self._s1 = s1
        self._s2 = s2
        self._s1.next()

    def before_first(self) -> None:
        self._s1.before_first()
        self._s1.next()
        self._s2.before_first()

    def next(self) -> bool:
        if self._s2.next():
            return True
        else:
            self._s2.before_first()
            return self._s2.next() and self._s1.next()

    def get_int(self, fldname: str) -> int:
        if self._s1.has_field(fldname):
            return self._s1.get_int(fldname)
        elif self._s2.has_field(fldname):
            return self._s2.get_int(fldname)
        else:
            raise RuntimeError(f"field {fldname} not found")

    def get_string(self, fldname: str) -> str:
        if self._s1.has_field(fldname):
            return self._s1.get_string(fldname)
        elif self._s2.has_field(fldname):
            return self._s2.get_string(fldname)
        else:
            raise RuntimeError(f"field {fldname} not found")

    def get_val(self, fldname: str) -> Constant:
        if self._s1.has_field(fldname):
            return self._s1.get_val(fldname)
        elif self._s2.has_field(fldname):
            return self._s2.get_val(fldname)
        else:
            raise RuntimeError(f"field {fldname} not found")

    def has_field(self, fldname: str) -> bool:
        return self._s1.has_field(fldname) or self._s2.has_field(fldname)

    def close(self) -> None:
        self._s1.close()
        self._s2.close()
