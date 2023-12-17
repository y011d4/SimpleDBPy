import unittest
from pathlib import Path

from simpledbpy.file import BlockId, FileMgr, Page


class TestFile(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/filetest"), 400)
        blk = BlockId("testfile", 2)
        p1 = Page(fm.block_size)
        pos1 = 88
        p1.set_string(pos1, "abcdefghijklm")
        size = Page.max_length(len("abcdefghijklm"))
        pos2 = pos1 + size
        p1.set_int(pos2, 345)
        fm.write(blk, p1)

        p2 = Page(fm.block_size)
        fm.read(blk, p2)
        assert p2.get_int(pos2) == 345
        assert p2.get_string(pos1) == "abcdefghijklm"
