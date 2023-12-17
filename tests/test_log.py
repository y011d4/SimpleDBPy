import unittest
from pathlib import Path

from simpledbpy.file import FileMgr, Page
from simpledbpy.log import LogMgr


class TestLog(unittest.TestCase):
    def test(self) -> None:
        fm = FileMgr(Path("/tmp/logtest"), 400)
        self._lm = LogMgr(fm, "temp_log")
        self._create_records(1, 35)
        self._print_log_records()
        self._create_records(36, 70)
        print(self._lm._latest_lsn)
        print(self._lm._last_saved_lsn)
        # print を見た限りではこの時点で63までしか保存されていない
        # ここで flush しなくても iter をつくるときに flush される
        self._lm.flush(65)
        self._print_log_records()

    def _print_log_records(self):
        for rec in self._lm:
            p = Page(rec)
            s = p.get_string(0)
            npos = Page.max_length(len(s))
            val = p.get_int(npos)
            print(f"[{s}, {val}]")

    def _create_records(self, start: int, end: int):
        for i in range(start, end + 1):
            rec = self._create_log_record(f"record{i}", i + 100)
            _ = self._lm.append(rec)

    def _create_log_record(self, s: str, n: int) -> bytes:
        npos = Page.max_length(len(s))
        b = bytes(npos + 4)
        p = Page(b)
        p.set_string(0, s)
        p.set_int(npos, n)
        return bytes(p.contents())
