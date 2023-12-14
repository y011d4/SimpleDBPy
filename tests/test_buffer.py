import threading
import time
import unittest
from pathlib import Path
from typing import Optional

from rdbpy.buffer import Buffer, BufferAbortException, BufferMgr
from rdbpy.file import BlockId, FileMgr
from rdbpy.log import LogMgr


class TestBuffer(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/buffertest"), 400)
        lm = LogMgr(fm, "testlog")
        bm = BufferMgr(fm, lm, 3)
        buff1 = bm.pin(BlockId("testfile", 1))
        p = buff1.contents
        n = p.get_int(80)
        print(n)
        p.set_int(80, n + 1)
        buff1.set_modified(1, 0)
        bm.unpin(buff1)
        buff2 = bm.pin(BlockId("testfile", 2))
        buff3 = bm.pin(BlockId("testfile", 3))
        buff4 = bm.pin(BlockId("testfile", 4))

        bm.unpin(buff2)
        buff2 = bm.pin(BlockId("testfile", 1))
        p2 = buff2.contents
        p2.set_int(80, 9999)
        buff2.set_modified(1, 0)
        bm.unpin(buff2)


class TestBufferMgr(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/buffertest"), 400)
        lm = LogMgr(fm, "testlog")
        bm = BufferMgr(fm, lm, 3)

        buff: list[Optional[Buffer]] = [None] * 6
        buff[0] = bm.pin(BlockId("testfile", 0))
        buff[1] = bm.pin(BlockId("testfile", 1))
        buff[2] = bm.pin(BlockId("testfile", 2))
        bm.unpin(buff[1])
        buff[1] = None
        buff[3] = bm.pin(BlockId("testfile", 0))
        buff[4] = bm.pin(BlockId("testfile", 1))
        print(bm.available)

        def unpin_after_sec(sec: int):
            time.sleep(sec)
            bm.unpin(buff[2])

        t = threading.Thread(target=unpin_after_sec, args=(3,))
        t.start()
        try:
            buff[5] = bm.pin(BlockId("testfile", 3))
        except BufferAbortException:
            pass
        t.join()
        bm.unpin(buff[2])
        buff[2] = None
        buff[5] = bm.pin(BlockId("testfile", 3))
        for i, b in enumerate(buff):
            if b is not None:
                print(f"buff[{i}] pinned to block {b.block}")
