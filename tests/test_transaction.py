import threading
import time
import unittest
from pathlib import Path
from rdbpy.buffer import BufferMgr

from rdbpy.file import BlockId, FileMgr
from rdbpy.log import LogMgr
from rdbpy.transaction import Transaction


class TxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fm = FileMgr(Path("/tmp/txtest"), 400)
        self.lm = LogMgr(self.fm, "test_log")
        self.bm = BufferMgr(self.fm, self.lm, 8)
        self.blk = BlockId("testfile", 1)

        tx1 = Transaction(self.fm, self.lm, self.bm)
        tx1.pin(self.blk)
        tx1.set_int(self.blk, 80, 1, False)
        tx1.set_string(self.blk, 40, "one", False)
        tx1.commit()

    def test_serialized(self) -> None:
        tx2 = Transaction(self.fm, self.lm, self.bm)
        tx2.pin(self.blk)
        ival = tx2.get_int(self.blk, 80)
        sval = tx2.get_string(self.blk, 40)
        print(f"initial value at location 80 = {ival}")
        print(f"initial value at location 40 = {sval}")
        newival = ival + 1
        newsval = sval + "!"
        tx2.set_int(self.blk, 80, newival, True)
        tx2.set_string(self.blk, 40, newsval, True)
        tx2.commit()

        tx3 = Transaction(self.fm, self.lm, self.bm)
        tx3.pin(self.blk)
        print(f"new value at location 80 = {tx3.get_int(self.blk, 80)}")
        print(f"new value at location 40 = {tx3.get_string(self.blk, 40)}")
        tx3.set_int(self.blk, 80, 9999, True)
        print(f"pre-rollback value at location 80 = {tx3.get_int(self.blk, 80)}")
        tx3.rollback()

        tx4 = Transaction(self.fm, self.lm, self.bm)
        tx4.pin(self.blk)
        print(f"post-rollback value at location 80 = {tx4.get_int(self.blk, 80)}")
        tx4.commit()

    def test_rw(self) -> None:
        # r2(b) -> w3(b) -> r2(b) -> c3 -> r2(b)
        # But this will be serialized as follows:
        # r2(b) -> r2(b) -> r2(b) -> w3(b) -> c3
        def read():
            tx2 = Transaction(self.fm, self.lm, self.bm)
            tx2.pin(self.blk)
            print(f"tx2: initial value at location 80 = {tx2.get_int(self.blk, 80)}")
            print(f"tx2: initial value at location 40 = {tx2.get_string(self.blk, 40)}")
            time.sleep(2)
            print(f"tx2: uncommited value at location 80 = {tx2.get_int(self.blk, 80)}")
            print(
                f"tx2: uncommited value at location 40 = {tx2.get_string(self.blk, 40)}"
            )
            time.sleep(2)
            print(f"tx2: commited value at location 80 = {tx2.get_int(self.blk, 80)}")
            print(
                f"tx2: commited value at location 40 = {tx2.get_string(self.blk, 40)}"
            )
            tx2.commit()

        def write():
            time.sleep(1)
            tx3 = Transaction(self.fm, self.lm, self.bm)
            tx3.pin(self.blk)
            ival = tx3.get_int(self.blk, 80)
            sval = tx3.get_string(self.blk, 40)
            newival = ival + 1
            newsval = sval + "!"
            tx3.set_int(self.blk, 80, newival, True)
            tx3.set_string(self.blk, 40, newsval, True)
            print(f"t3: write value at location 80 = {newival}")
            print(f"t3: write value at location 40 = {newsval}")
            time.sleep(2)
            tx3.commit()
            print("t3: commit")

        t2 = threading.Thread(target=read)
        t3 = threading.Thread(target=write)
        t2.start()
        t3.start()
        t2.join()
        t3.join()
