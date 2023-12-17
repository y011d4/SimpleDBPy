from dataclasses import dataclass
from threading import Lock
from typing import Mapping

from rdbpy.record import Layout, Schema, Types
from rdbpy.scan import TableScan
from rdbpy.transaction import Transaction


class TableMgr:
    """
    tblcat.tbl
    USED or EMPTY || tblname || slot_size

    fldcat.tbl
    USED or EMPTY || tblname || fldname || type || length || offset
    """

    MAX_NAME = 16

    _tcat_layout: Layout
    _fcat_layout: Layout

    def __init__(self, is_new: bool, tx: Transaction) -> None:
        tcat_schema = Schema()
        tcat_schema.add_string_field("tblname", TableMgr.MAX_NAME)
        tcat_schema.add_int_field("slotsize")
        self._tcat_layout = Layout.from_schema(tcat_schema)

        fcat_schema = Schema()
        fcat_schema.add_string_field("tblname", TableMgr.MAX_NAME)
        fcat_schema.add_string_field("fldname", TableMgr.MAX_NAME)
        fcat_schema.add_int_field("type")
        fcat_schema.add_int_field("length")
        fcat_schema.add_int_field("offset")
        self._fcat_layout = Layout.from_schema(fcat_schema)

        if is_new:
            self.create_table("tblcat", tcat_schema, tx)
            self.create_table("fldcat", fcat_schema, tx)

    def create_table(self, tblname: str, sch: Schema, tx: Transaction) -> None:
        layout = Layout.from_schema(sch)

        tcat = TableScan(tx, "tblcat", self._tcat_layout)
        tcat.insert()
        tcat.set_string("tblname", tblname)
        tcat.set_int("slotsize", layout.slot_size())
        tcat.close()

        fcat = TableScan(tx, "fldcat", self._fcat_layout)
        for fldname in sch.fields():
            fcat.insert()
            fcat.set_string("tblname", tblname)
            fcat.set_string("fldname", fldname)
            fcat.set_int("type", sch.type(fldname).value)
            fcat.set_int("length", sch.length(fldname))
            fcat.set_int("offset", layout.offset(fldname))
        fcat.close()

    def get_layout(self, tblname: str, tx: Transaction) -> Layout:
        size = -1
        tcat = TableScan(tx, "tblcat", self._tcat_layout)
        while tcat.next():
            if tcat.get_string("tblname") == tblname:
                size = tcat.get_int("slotsize")
                break
        tcat.close()

        sch = Schema()
        offsets = {}
        fcat = TableScan(tx, "fldcat", self._fcat_layout)
        while fcat.next():
            if fcat.get_string("tblname") == tblname:
                fldname = fcat.get_string("fldname")
                fldtype = Types(fcat.get_int("type"))
                fldlen = fcat.get_int("length")
                offset = fcat.get_int("offset")
                offsets[fldname] = offset
                sch.add_field(fldname, fldtype, fldlen)
        fcat.close()
        return Layout(sch, offsets, size)


class ViewMgr:
    MAX_VIEWDEF = 100

    _tbl_mgr: TableMgr

    def __init__(self, is_new: bool, tbl_mgr: TableMgr, tx: Transaction) -> None:
        self._tbl_mgr = tbl_mgr
        if is_new:
            sch = Schema()
            sch.add_string_field("viewname", TableMgr.MAX_NAME)
            sch.add_string_field("viewdef", ViewMgr.MAX_VIEWDEF)
            self._tbl_mgr.create_table("viewcat", sch, tx)

    def create_view(self, vname: str, vdef: str, tx: Transaction) -> None:
        layout = self._tbl_mgr.get_layout("viewcat", tx)
        ts = TableScan(tx, "viewcat", layout)
        ts.insert()
        ts.set_string("viewname", vname)
        ts.set_string("viewdef", vdef)
        ts.close()

    def get_view_def(self, vname: str, tx: Transaction) -> str:
        result = None
        layout = self._tbl_mgr.get_layout("viewcat", tx)
        ts = TableScan(tx, "viewcat", layout)
        while ts.next():
            if ts.get_string("viewname") == vname:
                result = ts.get_string("viewdef")
                break
        ts.close()
        assert result is not None
        return result


@dataclass
class StatInfo:
    num_blocks: int
    num_recs: int

    def distinct_values(self, fldname: str) -> int:
        return 1 + (self.num_recs // 3)  # NOTE: This is wildly inaccurate.


class StatMgr:
    _tbl_mgr: TableMgr
    _tablestats: dict[str, StatInfo]
    _numcalls: int
    _lock: Lock

    def __init__(self, tbl_mgr: TableMgr, tx: Transaction) -> None:
        self._tbl_mgr = tbl_mgr
        self._lock = Lock()
        self._refresh_statistics(tx)

    def get_stat_info(self, tblname: str, layout: Layout, tx: Transaction) -> StatInfo:
        with self._lock:
            self._numcalls += 1
            if self._numcalls > 100:
                self._refresh_statistics(tx)
            si = self._tablestats.get(tblname)
            if si is None:
                si = self._calc_table_stats(tblname, layout, tx)
                self._tablestats[tblname] = si
            return si

    def _refresh_statistics(self, tx: Transaction) -> None:
        with self._lock:
            tablestats = {}
            self._numcalls = 0
            tcatlayout = self._tbl_mgr.get_layout("tblcat", tx)
            tcat = TableScan(tx, "tblcat", tcatlayout)
            while tcat.next():
                tblname = tcat.get_string("tblname")
                layout = self._tbl_mgr.get_layout(tblname, tx)
                si = self._calc_table_stats(tblname, layout, tx)
                tablestats[tblname] = si
            tcat.close()
            self._tablestats = tablestats

    def _calc_table_stats(
        self, tblname: str, layout: Layout, tx: Transaction
    ) -> StatInfo:
        num_recs = 0
        numblocks = 0
        ts = TableScan(tx, tblname, layout)
        while ts.next():
            num_recs += 1
            numblocks = ts.get_rid().blknum + 1
        ts.close()
        return StatInfo(num_blocks=numblocks, num_recs=num_recs)


class IndexInfo:
    _idxname: str
    _fldname: str
    _tbl_schema: Schema
    _tx: Transaction
    _idx_layout: Layout
    _si: StatInfo

    def __init__(
        self,
        idxname: str,
        fldname: str,
        tbl_schema: Schema,
        tx: Transaction,
        si: StatInfo,
    ) -> None:
        self._idxname = idxname
        self._fldname = fldname
        self._tbl_schema = tbl_schema
        self._tx = tx
        self._si = si
        self._create_idx_layout()

    # def open(self) -> Index:
    #     sch = Schema()
    #     return HashIndex(self._tx, self._idxname, self._idx_layout)

    # def blocks_accessed(self) -> int:
    #     rpb = self._tx.block_size() // self._idx_layout.slot_size()
    #     numblocks = self._si.num_recs // rpb
    #     return HashIndex.search_cost(numblocks, rpb)

    def records_output(self) -> int:
        return self._si.num_recs // self._si.distinct_values(self._fldname)

    def distinct_values(self, fname: str) -> int:
        return 1 if self._fldname == fname else self._si.distinct_values(self._fldname)

    def _create_idx_layout(self) -> Layout:
        sch = Schema()
        sch.add_int_field("block")
        sch.add_int_field("id")
        fldtype = self._tbl_schema.type(self._fldname)
        if fldtype == Types.INTEGER:
            sch.add_int_field("dataval")
        elif fldtype == Types.VARCHAR:
            fldlen = self._tbl_schema.length(self._fldname)
            sch.add_string_field(self._fldname, fldlen)
        else:
            raise ValueError
        return Layout.from_schema(sch)


class IndexMgr:
    _layout: Layout
    _tblmgr: TableMgr
    _statmgr: StatMgr

    def __init__(
        self, isnew: bool, tblmgr: TableMgr, statmgr: StatMgr, tx: Transaction
    ) -> None:
        if isnew:
            sch = Schema()
            sch.add_string_field("indexname", TableMgr.MAX_NAME)
            sch.add_string_field("tablename", TableMgr.MAX_NAME)
            sch.add_string_field("fieldname", TableMgr.MAX_NAME)
            tblmgr.create_table("idxcat", sch, tx)
        self._tblmgr = tblmgr
        self._statmgr = statmgr
        self._layout = tblmgr.get_layout("idxcat", tx)

    def create_index(
        self, idxname: str, tblname: str, fldname: str, tx: Transaction
    ) -> None:
        ts = TableScan(tx, "idxcat", self._layout)
        ts.insert()
        ts.set_string("indexname", idxname)
        ts.set_string("tablename", tblname)
        ts.set_string("fieldname", fldname)
        ts.close()

    def get_index_info(self, tblname: str, tx: Transaction) -> Mapping[str, IndexInfo]:
        result = {}
        ts = TableScan(tx, "idxcat", self._layout)
        while ts.next():
            if ts.get_string("tablename") == tblname:
                idxname = ts.get_string("indexname")
                fldname = ts.get_string("fieldname")
                tbl_layout = self._tblmgr.get_layout(tblname, tx)
                tblsi = self._statmgr.get_stat_info(tblname, tbl_layout, tx)
                ii = IndexInfo(idxname, fldname, tbl_layout.schema(), tx, tblsi)
                result[fldname] = ii
        ts.close()
        return result


class MetadataMgr:
    _tblmgr: TableMgr
    _viewmgr: ViewMgr
    _statmgr: StatMgr
    _idxmgr: IndexMgr

    def __init__(self, isnew: bool, tx: Transaction) -> None:
        self._tblmgr = TableMgr(isnew, tx)
        self._viewmgr = ViewMgr(isnew, self._tblmgr, tx)
        self._statmgr = StatMgr(self._tblmgr, tx)
        self._idxmgr = IndexMgr(isnew, self._tblmgr, self._statmgr, tx)

    def create_table(self, tblname: str, sch: Schema, tx: Transaction) -> None:
        self._tblmgr.create_table(tblname, sch, tx)

    def get_layout(self, tblname: str, tx: Transaction) -> Layout:
        return self._tblmgr.get_layout(tblname, tx)

    def create_view(self, viewname: str, viewdef: str, tx: Transaction) -> None:
        return self._viewmgr.create_view(viewname, viewdef, tx)

    def get_view_def(self, viewname: str, tx: Transaction) -> str:
        return self._viewmgr.get_view_def(viewname, tx)

    def create_index(
        self, idxname: str, tblname: str, fldname: str, tx: Transaction
    ) -> None:
        self._idxmgr.create_index(idxname, tblname, fldname, tx)

    def get_index_info(self, tblname: str, tx: Transaction) -> Mapping[str, IndexInfo]:
        return self._idxmgr.get_index_info(tblname, tx)

    def get_stat_info(self, tblname: str, layout: Layout, tx: Transaction) -> StatInfo:
        return self._statmgr.get_stat_info(tblname, layout, tx)
