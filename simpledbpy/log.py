from threading import Lock
from simpledbpy.file import BlockId, FileMgr, Page


class LogMgr:
    _fm: FileMgr
    _logfile: str
    _logpage: Page
    _currentblk: BlockId
    _latest_lsn: int
    _last_saved_lsn: int
    _lock: Lock

    def __init__(self, fm: FileMgr, logfile: str) -> None:
        self._fm = fm
        self._logfile = logfile
        b = bytes(self._fm.block_size)
        self._logpage = Page(b)
        logsize = fm.length(logfile)
        if logsize == 0:
            self._currentblk = self._append_new_block()
        else:
            self._currentblk = BlockId(self._logfile, logsize - 1)
            fm.read(self._currentblk, self._logpage)
        self._latest_lsn = 0
        self._last_saved_lsn = 0
        self._lock = Lock()

    def flush(self, lsn: int) -> None:
        if lsn >= self._last_saved_lsn:
            self._flush()

    def __iter__(self):
        self._flush()
        return LogIterator(self._fm, self._currentblk)

    def append(self, logrec: bytes) -> int:
        with self._lock:
            boundary = self._logpage.get_int(0)
            recsize = len(logrec)
            bytesneeded = recsize + 4
            if boundary - bytesneeded < 4:
                self._flush()
                self._currentblk = self._append_new_block()
                boundary = self._logpage.get_int(0)
            recpos = boundary - bytesneeded
            self._logpage.set_bytes(recpos, logrec)
            self._logpage.set_int(0, recpos)
            self._latest_lsn += 1
            return self._latest_lsn

    def _append_new_block(self) -> BlockId:
        blk = self._fm.append(self._logfile)
        self._logpage.set_int(0, self._fm.block_size)
        self._fm.write(blk, self._logpage)
        return blk

    def _flush(self) -> None:
        self._fm.write(self._currentblk, self._logpage)
        self._last_saved_lsn = self._latest_lsn


class LogIterator:
    _fm: FileMgr
    _blk: BlockId
    _p: Page
    _currentpos: int
    _boundary: int

    def __init__(self, fm: FileMgr, blk: BlockId) -> None:
        self._fm = fm
        self._blk = blk
        b = bytes(self._fm.block_size)
        self._p = Page(b)
        self._move_to_block(self._blk)

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self._currentpos < self._fm.block_size or self._blk.blknum > 0:
            if self._currentpos == self._fm.block_size:
                self._blk = BlockId(self._blk.filename, self._blk.blknum - 1)
                self._move_to_block(self._blk)
            rec = self._p.get_bytes(self._currentpos)
            self._currentpos += len(rec) + 4
            return rec
        else:
            raise StopIteration

    def _move_to_block(self, blk: BlockId) -> None:
        self._fm.read(blk, self._p)
        self._boundary = self._p.get_int(0)
        self._currentpos = self._boundary


"""
区切りは page size
page の先頭は残りバイト数
00000000: 2800 0000 0000 0000 0000 0000 0000 0000  (...............
00000010: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000020: 0000 0000 0000 0000 2400 0000 0700 0000  ........$.......
00000030: 7265 636f 7264 3900 0000 0000 0000 0000  record9.........
00000040: 0000 0000 0000 0000 0000 0000 6d00 0000  ............m...
00000050: 2400 0000 0700 0000 7265 636f 7264 3800  $.......record8.
00000060: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000070: 0000 0000 6c00 0000 2400 0000 0700 0000  ....l...$.......
00000080: 7265 636f 7264 3700 0000 0000 0000 0000  record7.........
00000090: 0000 0000 0000 0000 0000 0000 6b00 0000  ............k...
000000a0: 2400 0000 0700 0000 7265 636f 7264 3600  $.......record6.
000000b0: 0000 0000 0000 0000 0000 0000 0000 0000  ................
000000c0: 0000 0000 6a00 0000 2400 0000 0700 0000  ....j...$.......
000000d0: 7265 636f 7264 3500 0000 0000 0000 0000  record5.........
000000e0: 0000 0000 0000 0000 0000 0000 6900 0000  ............i...
000000f0: 2400 0000 0700 0000 7265 636f 7264 3400  $.......record4.
00000100: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000110: 0000 0000 6800 0000 2400 0000 0700 0000  ....h...$.......
00000120: 7265 636f 7264 3300 0000 0000 0000 0000  record3.........
00000130: 0000 0000 0000 0000 0000 0000 6700 0000  ............g...
00000140: 2400 0000 0700 0000 7265 636f 7264 3200  $.......record2.
00000150: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000160: 0000 0000 6600 0000 2400 0000 0700 0000  ....f...$.......
00000170: 7265 636f 7264 3100 0000 0000 0000 0000  record1.........
00000180: 0000 0000 0000 0000 0000 0000 6500 0000  ............e...

00000190: 0400 0000 2800 0000 0800 0000 7265 636f  ....(.......reco
000001a0: 7264 3138 0000 0000 0000 0000 0000 0000  rd18............
000001b0: 0000 0000 0000 0000 0000 0000 7600 0000  ............v...
000001c0: 2800 0000 0800 0000 7265 636f 7264 3137  (.......record17
000001d0: 0000 0000 0000 0000 0000 0000 0000 0000  ................
000001e0: 0000 0000 0000 0000 7500 0000 2800 0000  ........u...(...
000001f0: 0800 0000 7265 636f 7264 3136 0000 0000  ....record16....
00000200: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000210: 0000 0000 7400 0000 2800 0000 0800 0000  ....t...(.......
00000220: 7265 636f 7264 3135 0000 0000 0000 0000  record15........
00000230: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000240: 7300 0000 2800 0000 0800 0000 7265 636f  s...(.......reco
00000250: 7264 3134 0000 0000 0000 0000 0000 0000  rd14............
00000260: 0000 0000 0000 0000 0000 0000 7200 0000  ............r...
00000270: 2800 0000 0800 0000 7265 636f 7264 3133  (.......record13
00000280: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000290: 0000 0000 0000 0000 7100 0000 2800 0000  ........q...(...
000002a0: 0800 0000 7265 636f 7264 3132 0000 0000  ....record12....
000002b0: 0000 0000 0000 0000 0000 0000 0000 0000  ................
000002c0: 0000 0000 7000 0000 2800 0000 0800 0000  ....p...(.......
000002d0: 7265 636f 7264 3131 0000 0000 0000 0000  record11........
000002e0: 0000 0000 0000 0000 0000 0000 0000 0000  ................
000002f0: 6f00 0000 2800 0000 0800 0000 7265 636f  o...(.......reco
00000300: 7264 3130 0000 0000 0000 0000 0000 0000  rd10............
00000310: 0000 0000 0000 0000 0000 0000 6e00 0000  ............n...

00000320: 0400 0000 2800 0000 0800 0000 7265 636f  ....(.......reco
00000330: 7264 3237 0000 0000 0000 0000 0000 0000  rd27............
00000340: 0000 0000 0000 0000 0000 0000 7f00 0000  ................
00000350: 2800 0000 0800 0000 7265 636f 7264 3236  (.......record26
00000360: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000370: 0000 0000 0000 0000 7e00 0000 2800 0000  ........~...(...
00000380: 0800 0000 7265 636f 7264 3235 0000 0000  ....record25....
00000390: 0000 0000 0000 0000 0000 0000 0000 0000  ................
000003a0: 0000 0000 7d00 0000 2800 0000 0800 0000  ....}...(.......
"""
