from pathlib import Path
from simpledbpy.buffer import BufferMgr

from simpledbpy.file import FileMgr
from simpledbpy.log import LogMgr
from simpledbpy.metadata import MetadataMgr
from simpledbpy.transaction import Transaction


class SimpleDB:
    BLOCK_SIZE = 512
    BUFFER_SIZE = 8
    LOG_FILE = "simpledb.log"

    _fm: FileMgr
    _lm: LogMgr
    _bm: BufferMgr
    _mdm: MetadataMgr

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
        tx.commit()
