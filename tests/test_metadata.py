import random
import unittest
import shutil
from pathlib import Path
from rdbpy.buffer import BufferMgr

from rdbpy.file import FileMgr
from rdbpy.log import LogMgr
from rdbpy.metadata import MetadataMgr, TableMgr
from rdbpy.record import Schema, Types
from rdbpy.recovery import LogRecord
from rdbpy.scan import TableScan
from rdbpy.transaction import Transaction


class TestTableMgr(unittest.TestCase):
    def test(self) -> None:
        path = Path("/tmp/metadatatest")
        shutil.rmtree(path)
        fm = FileMgr(Path("/tmp/metadatatest"), 400)
        lm = LogMgr(fm, "test_log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        sch = Schema()
        sch.add_int_field("A")
        sch.add_string_field("B", 9)
        tm = TableMgr(True, tx)
        tm.create_table("MyTable", sch, tx)

        layout = tm.get_layout("MyTable", tx)
        size = layout.slot_size()
        sch2 = layout.schema()
        print(f"MyTable has slot size {size}")
        print("Its fields are:")
        for fldname in sch2.fields():
            fldtype = sch2.type(fldname)
            if fldtype == Types.INTEGER:
                type = "int"
            elif fldtype == Types.VARCHAR:
                strlen = sch2.length(fldname)
                type = f"varchar({strlen})"
            else:
                raise ValueError
            print(f"{fldname}: {type}")
        tx.commit()

        for buff in lm:
            print(LogRecord.create_log_record(buff))


class TestMetadataMgr(unittest.TestCase):
    def test(self) -> None:
        path = Path("/tmp/metadatatest")
        shutil.rmtree(path)
        fm = FileMgr(Path("/tmp/metadatatest"), 512)
        lm = LogMgr(fm, "test_log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)
        mdm = MetadataMgr(True, tx)

        sch = Schema()
        sch.add_int_field("A")
        sch.add_string_field("B", 9)

        # Table Metadata
        mdm.create_table("MyTable", sch, tx)
        layout = mdm.get_layout("MyTable", tx)
        size = layout.slot_size()
        sch2 = layout.schema()
        print(f"MyTable has slot size {size}")
        print("Its fields are:")
        for fldname in sch2.fields():
            fldtype = sch2.type(fldname)
            if fldtype == Types.INTEGER:
                type = "int"
            elif fldtype == Types.VARCHAR:
                strlen = sch2.length(fldname)
                type = f"varchar({strlen})"
            else:
                raise ValueError
            print(f"{fldname}: {type}")

        # Statistics Metadata
        ts = TableScan(tx, "MyTable", layout)
        for _ in range(50):
            ts.insert()
            n = int(random.random() * 50)
            ts.set_int("A", n)
            ts.set_string("B", f"rec{n}")
        si = mdm.get_stat_info("MyTable", layout, tx)
        print(f"B(MyTable) = {si.num_blocks}")
        print(f"R(MyTable) = {si.num_recs}")
        print(f"V(MyTable, A) = {si.distinct_values('A')}")
        print(f"V(MyTable, B) = {si.distinct_values('B')}")

        # View Metadata
        viewdef = "select B from MyTable where A = 1"
        mdm.create_view("viewA", viewdef, tx)
        v = mdm.get_view_def("viewA", tx)
        print(f"View def = {v}")

        # Index Metadata
        mdm.create_index("indexA", "MyTable", "A", tx)
        mdm.create_index("indexB", "MyTable", "B", tx)
        idxmap = mdm.get_index_info("MyTable", tx)
        ii = idxmap.get("A")
        assert ii is not None
        # print(f"B(indexA) = {ii.blocks_accessed()}")
        print(f"R(indexA) = {ii.records_output()}")
        print(f"V(indexA, A) = {ii.distinct_values('A')}")
        print(f"V(indexA, B) = {ii.distinct_values('B')}")
        tx.commit()
