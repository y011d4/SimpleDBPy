import random
import unittest
from pathlib import Path
from rdbpy.buffer import BufferMgr

from rdbpy.file import FileMgr
from rdbpy.log import LogMgr
from rdbpy.record import Layout, Schema
from rdbpy.scan import TableScan
from rdbpy.transaction import Transaction


class TestScan(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/scantest"), 400)
        lm = LogMgr(fm, "test_log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        sch = Schema()
        sch.add_int_field("A")
        sch.add_string_field("B", 9)
        layout = Layout.from_schema(sch)
        for fldname in layout.schema().fields():
            offset = layout.offset(fldname)
            print(f"{fldname} has offset {offset}")

        ts = TableScan(tx, "T", layout)
        ts.before_first()
        while ts.next():
            ts.delete()
        print("Filling the page with random records.")
        ts.before_first()
        for _ in range(50):
            ts.insert()
            n = int(random.random() * 50)
            ts.set_int("A", n)
            ts.set_string("B", f"rec{n}")
            print(f"inserting into slot {ts.get_rid()}: ({n}, rec{n})")
        print("Deleted these records with A-values < 25.")
        count = 0
        ts.before_first()
        while ts.next():
            a = ts.get_int("A")
            b = ts.get_string("B")
            if a < 25:
                count += 1
                print(f"slot {ts.get_rid()}: ({a}, {b})")
                ts.delete()
        print(f"{count} values under 25 were deleted.\n")
        print("Here are the remaining records.")
        ts.before_first()
        while ts.next():
            a = ts.get_int("A")
            b = ts.get_string("B")
            print(f"slot {ts.get_rid()}: ({a}, {b})")
        ts.close()
        tx.commit()
