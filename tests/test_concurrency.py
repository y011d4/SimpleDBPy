import threading
import time
import unittest
from pathlib import Path
from simpledbpy.buffer import BufferMgr
from simpledbpy.concurrency import LockAbortError

from simpledbpy.file import BlockId, FileMgr
from simpledbpy.log import LogMgr
from simpledbpy.transaction import Transaction


class ConcurrencyTest(unittest.TestCase):
    def test(self) -> None:
        # txA: sLock(blk1); sLock(blk2); unlock(blk1); unlock(blk2)
        # txB: xLock(blk2); sLock(blk1); unlock(blk1); unlock(blk2)
        # txC: xLock(blk1); sLock(blk2); unlock(blk1); unlock(blk2)
        fm = FileMgr(Path("/tmp/concurrencytest"), 400)
        lm = LogMgr(fm, "test_log")
        bm = BufferMgr(fm, lm, 8)

        def a():
            tx_a = Transaction(fm, lm, bm)
            try:
                blk1 = BlockId("testfile", 1)
                blk2 = BlockId("testfile", 2)
                tx_a.pin(blk1)
                tx_a.pin(blk2)
                print("Tx A: request slock 1")
                tx_a.get_int(blk1, 0)
                print("Tx A: receive slock 1")
                time.sleep(1)
                print("Tx A: request slock 2")
                tx_a.get_int(blk2, 0)
                print("Tx A: receive slock 2")
                tx_a.commit()
            except LockAbortError:
                tx_a.rollback()

        def b():
            tx_b = Transaction(fm, lm, bm)
            try:
                blk1 = BlockId("testfile", 1)
                blk2 = BlockId("testfile", 2)
                tx_b.pin(blk1)
                tx_b.pin(blk2)
                print("Tx B: request xlock 2")
                tx_b.set_int(blk2, 0, 0, False)
                print("Tx B: receive xlock 2")
                time.sleep(1)
                print("Tx B: request slock 1")
                tx_b.get_int(blk1, 0)
                # tx_b.set_int(blk1, 0, 0, False)
                print("Tx B: receive slock 1")
                tx_b.commit()
            except LockAbortError:
                tx_b.rollback()

        def c():
            tx_c = Transaction(fm, lm, bm)
            try:
                blk1 = BlockId("testfile", 1)
                blk2 = BlockId("testfile", 2)
                tx_c.pin(blk1)
                tx_c.pin(blk2)
                print("Tx C: request xlock 1")
                tx_c.set_int(blk1, 0, 0, False)
                print("Tx C: receive xlock 1")
                time.sleep(1)
                print("Tx C: request slock 2")
                tx_c.get_int(blk2, 0)
                print("Tx C: receive slock 2")
                tx_c.commit()
            except LockAbortError:
                tx_c.rollback()

        t_a = threading.Thread(target=a)
        t_b = threading.Thread(target=b)
        t_c = threading.Thread(target=c)
        t_a.start()
        time.sleep(0.1)
        t_b.start()
        time.sleep(0.1)
        t_c.start()
        t_a.join()
        t_b.join()
        t_c.join()
