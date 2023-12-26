import unittest
from pathlib import Path

from simpledbpy.buffer import BufferMgr
from simpledbpy.file import BlockId, FileMgr
from simpledbpy.log import LogMgr
from simpledbpy.metadata import MetadataMgr
from simpledbpy.plan import BasicQueryPlanner, BasicUpdatePlanner, BetterQueryPlanner, Planner
from simpledbpy.transaction import Transaction


class TestPlanner(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/plantest"), 512)
        lm = LogMgr(fm, "temp_log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)
        isnew = fm.is_new
        if isnew:
            print("creating new database")
        else:
            print("recovering existing database")
            tx.recover()
        mdm = MetadataMgr(isnew, tx)
        qp = BasicQueryPlanner(mdm)
        up = BasicUpdatePlanner(mdm)
        planner = Planner(qp, up)
        tx.commit()

        if isnew:
            qry = "CREATE TABLE student (sname VARCHAR(32), gradyear INT)"
            planner.execute_update(qry, tx)
        qry = "INSERT INTO student(sname, gradyear) VALUES ('hogetaro', 1993)"
        print(planner.execute_update(qry, tx))
        qry = "INSERT INTO student(sname, gradyear) VALUES ('fugataro', 1992)"
        print(planner.execute_update(qry, tx))
        qry = "SELECT sname, gradyear FROM student"
        # qry = "SELECT sname FROM student"
        p = planner.create_query_plan(qry, tx)
        s = p.open()
        while s.next():
            print(f"{s.get_string('sname')} {s.get_int('gradyear')}")
            # print(f"{s.get_string('sname')}")
