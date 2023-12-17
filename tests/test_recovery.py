import os
from pathlib import Path
import unittest
from rdbpy.buffer import BufferMgr
from rdbpy.file import BlockId, FileMgr
from rdbpy.log import LogMgr

from rdbpy.recovery import RecoveryMgr
from rdbpy.transaction import Transaction


class TestRecovery(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/recoverytest"), 400)
        lm = LogMgr(fm, "temp_log")
        bm = BufferMgr(fm, lm, 8)
        tx_init = Transaction(fm, lm, bm)
        blk = BlockId("temp_file", 1)
        tx_init.pin(blk)
        tx_init.set_int(blk, 0, 0xffff, True)
        tx_init.set_string(blk, 100, "hogetaro", True)
        tx_init.commit()
        tx_broken = Transaction(fm, lm, bm)
        blk = BlockId("temp_file", 1)
        tx_broken.pin(blk)
        tx_broken.set_int(blk, 0, 0x1337, True)
        tx_broken.set_string(blk, 100, "fugajiro", True)
        # commit 直前のファイル書き込みまではできたが、 commit を log に書くのに失敗したケースを想定
        bm.flush_all(tx_broken._txnum)
        tx_broken._concur_mgr.release()
        # tx_init.commit()

        tx = Transaction(fm, lm, bm)
        tx.pin(blk)
        print(f"saved value at location 0 = {tx.get_int(blk, 0)}")
        print(f"saved value at location 100 = {tx.get_string(blk, 100)}")
        tx.recover()
        print(f"recovered value at location 0 = {tx.get_int(blk, 0)}")
        print(f"recovered value at location 100 = {tx.get_string(blk, 100)}")
