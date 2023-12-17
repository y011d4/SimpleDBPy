import random
import unittest
from pathlib import Path
from rdbpy.buffer import BufferMgr

from rdbpy.file import BlockId, FileMgr
from rdbpy.log import LogMgr
from rdbpy.record import Layout, RecordPage, Schema
from rdbpy.transaction import Transaction


class TestRecord(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/recordtest"), 400)
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
        blk = tx.append("testfile")
        tx.pin(blk)
        rp = RecordPage(tx, blk, layout)
        rp.format()

        print("Filling the page with random records.")
        slot = rp.insert_after(-1)
        while slot >= 0:
            n = int(random.random() * 50)
            rp.set_int(slot, "A", n)
            rp.set_string(slot, "B", f"rec{n}")
            print(f"inserting into slot {slot}: ({n}, rec{n})")
            slot = rp.insert_after(slot)
        print("Deleted these records with A-values < 25.")
        count = 0
        slot = rp.next_after(-1)
        while slot >= 0:
            a = rp.get_int(slot, "A")
            b = rp.get_string(slot, "B")
            if a < 25:
                count += 1
                print(f"slot {slot}: ({a}, {b})")
                rp.delete(slot)
            slot = rp.next_after(slot)
        print(f"{count} values under 25 were deleted.\n")
        print("Here are the remaining records.")
        slot = rp.next_after(-1)
        while slot >= 0:
            a = rp.get_int(slot, "A")
            b = rp.get_string(slot, "B")
            print(f"slot {slot}: ({a}, {b})")
            slot = rp.next_after(slot)
        tx.unpin(blk)
        tx.commit()
