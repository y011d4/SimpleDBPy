from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

from rdbpy.file import BlockId, Page
from rdbpy.transaction import Transaction


class Types(Enum):
    INTEGER = 0
    VARCHAR = 1


@dataclass
class FieldInfo:
    type: Types
    length: int


class Schema:
    _fields: list[str]
    _info: dict[str, FieldInfo]

    def __init__(self) -> None:
        self._fields = []
        self._info = {}

    def add_field(self, fldname: str, type: Types, length: int) -> None:
        self._fields.append(fldname)
        self._info[fldname] = FieldInfo(type=type, length=length)

    def add_int_field(self, fldname: str) -> None:
        self.add_field(fldname, Types.INTEGER, 0)

    def add_string_field(self, fldname: str, length: int) -> None:
        self.add_field(fldname, Types.VARCHAR, length)

    def add(self, fldname: str, sch: "Schema") -> None:
        type = sch.type(fldname)
        length = sch.length(fldname)
        self.add_field(fldname, type, length)

    def add_all(self, sch: "Schema") -> None:
        for fldname in sch.fields():
            self.add(fldname, sch)

    def fields(self) -> Sequence[str]:
        return self._fields

    def has_field(self, fldname: str) -> bool:
        return fldname in self._fields

    def type(self, fldname: str) -> Types:
        assert fldname in self._info
        return self._info[fldname].type

    def length(self, fldname: str) -> int:
        assert fldname in self._info
        return self._info[fldname].length


class Layout:
    _schema: Schema
    _offsets: Mapping[str, int]
    _slotsize: int

    def __init__(
        self, schema: Schema, offsets: Mapping[str, int], slotsize: int
    ) -> None:
        self._schema = schema
        self._offsets = offsets
        self._slotsize = slotsize

    @classmethod
    def from_schema(cls, schema: Schema) -> "Layout":
        offsets = {}
        pos = 4
        for fldname in schema.fields():
            offsets[fldname] = pos
            pos += Layout._length_in_bytes(fldname, schema)
        return Layout(schema=schema, offsets=offsets, slotsize=pos)

    def schema(self) -> Schema:
        return self._schema

    def offset(self, fldname: str) -> int:
        assert fldname in self._offsets
        return self._offsets[fldname]

    def slot_size(self) -> int:
        return self._slotsize

    @staticmethod
    def _length_in_bytes(fldname: str, schema: Schema) -> int:
        fldtype = schema.type(fldname)
        if fldtype == Types.INTEGER:
            return 4
        elif fldtype == Types.VARCHAR:
            return Page.max_length(schema.length(fldname))
        else:
            raise ValueError(f"{fldtype} is not yet defined. Please modify record.py.")


class RecordPage:
    EMPTY = 0
    USED = 1

    _tx: Transaction
    _blk: BlockId
    _layout: Layout

    def __init__(self, tx: Transaction, blk: BlockId, layout: Layout) -> None:
        self._tx = tx
        self._blk = blk
        self._layout = layout
        self._tx.pin(blk)

    def get_int(self, slot: int, fldname: str) -> int:
        fldpos = self._offset(slot) + self._layout.offset(fldname)
        return self._tx.get_int(self._blk, fldpos)

    def get_string(self, slot: int, fldname: str) -> str:
        fldpos = self._offset(slot) + self._layout.offset(fldname)
        return self._tx.get_string(self._blk, fldpos)

    def set_int(self, slot: int, fldname: str, val: int) -> None:
        fldpos = self._offset(slot) + self._layout.offset(fldname)
        self._tx.set_int(self._blk, fldpos, val, True)

    def set_string(self, slot: int, fldname: str, val: str) -> None:
        fldpos = self._offset(slot) + self._layout.offset(fldname)
        self._tx.set_string(self._blk, fldpos, val, True)

    def delete(self, slot: int) -> None:
        self._set_flag(slot, RecordPage.EMPTY)

    def format(self) -> None:
        slot = 0
        while self._is_valid_slot(slot):
            self._tx.set_int(self._blk, self._offset(slot), RecordPage.EMPTY, False)
            sch = self._layout.schema()
            for fldname in sch.fields():
                fldpos = self._offset(slot) + self._layout.offset(fldname)
                fldtype = sch.type(fldname)
                if fldtype == Types.INTEGER:
                    self._tx.set_int(self._blk, fldpos, 0, False)
                elif fldtype == Types.VARCHAR:
                    self._tx.set_string(self._blk, fldpos, "", False)
            slot += 1

    def next_after(self, slot: int) -> int:
        return self._search_after(slot, RecordPage.USED)

    def insert_after(self, slot: int) -> int:
        newslot = self._search_after(slot, RecordPage.EMPTY)
        if newslot >= 0:
            self._set_flag(newslot, RecordPage.USED)
        return newslot

    def block(self) -> BlockId:
        return self._blk

    def _set_flag(self, slot: int, flag: int) -> None:
        self._tx.set_int(self._blk, self._offset(slot), flag, True)

    def _search_after(self, slot: int, flag: int) -> int:
        slot += 1
        while self._is_valid_slot(slot):
            if self._tx.get_int(self._blk, self._offset(slot)) == flag:
                return slot
            slot += 1
        return -1

    def _is_valid_slot(self, slot: int) -> bool:
        return self._offset(slot + 1) <= self._tx.block_size()

    def _offset(self, slot: int) -> int:
        return slot * self._layout.slot_size()
