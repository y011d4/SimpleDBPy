from pathlib import Path
from simpledbpy.buffer import BufferMgr

from simpledbpy.file import FileMgr
from simpledbpy.log import LogMgr
from simpledbpy.metadata import MetadataMgr
from simpledbpy.plan import (
    BasicQueryPlanner,
    BasicUpdatePlanner,
    BetterQueryPlanner,
    Planner,
)
from simpledbpy.transaction import Transaction


class SimpleDB:
    BLOCK_SIZE = 512
    BUFFER_SIZE = 8
    LOG_FILE = "simpledb.log"

    _fm: FileMgr
    _lm: LogMgr
    _bm: BufferMgr
    _mdm: MetadataMgr
    _planner: Planner

    def __init__(self, dirname: str) -> None:
        tmpdir = Path("/tmp")
        self._fm = FileMgr(tmpdir / dirname, SimpleDB.BLOCK_SIZE)
        self._lm = LogMgr(self._fm, SimpleDB.LOG_FILE)
        self._bm = BufferMgr(self._fm, self._lm, SimpleDB.BUFFER_SIZE)
        tx = Transaction(self._fm, self._lm, self._bm)
        isnew = self._fm.is_new
        if isnew:
            print("creating new database")
        else:
            print("recovering existing database")
            tx.recover()
        self._mdm = MetadataMgr(isnew, tx)
        qp = BetterQueryPlanner(self._mdm)
        up = BasicUpdatePlanner(self._mdm)
        self._planner = Planner(qp, up)
        tx.commit()

    def new_tx(self) -> Transaction:
        return Transaction(self._fm, self._lm, self._bm)

    def planner(self) -> Planner:
        return self._planner
