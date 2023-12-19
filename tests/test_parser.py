import unittest

from simpledbpy.parser import Lexer, Parser


class TestParser(unittest.TestCase):
    def test(self) -> None:
        parser = Parser("SELECT a FROM tbl WHERE a=3")
        print(parser.query())
        parser = Parser("INSERT INTO tbl (a, b) VALUES (3, 'abcde')")
        print(parser.update_cmd())
        parser = Parser("DELETE FROM tbl WHERE b='3ijaofe' AND c=212")
        print(parser.update_cmd())
        parser = Parser("UPDATE tbl SET a=3 WHERE a=1 AND b='2aaaa'")
        print(parser.update_cmd())
        parser = Parser("CREATE TABLE tbl (a INT, b VARCHAR(255))")
        print(parser.update_cmd())
        parser = Parser("CREATE VIEW v AS SELECT hoge FROM tbl")
        print(parser.update_cmd())
        parser = Parser("CREATE INDEX idx ON tbl(a)")
        print(parser.update_cmd())
