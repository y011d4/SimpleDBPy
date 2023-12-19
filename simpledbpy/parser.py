import re
from dataclasses import dataclass
from typing import Collection, Iterator, Sequence

from simpledbpy.grammar import Constant, Expression, Term
from simpledbpy.record import Schema
from simpledbpy.scan import Predicate


class BadSyntaxError(RuntimeError):
    pass


class Lexer:
    KEYWORDS: Collection[str] = {
        "select",
        "from",
        "where",
        "and",
        "insert",
        "into",
        "values",
        "delete",
        "update",
        "set",
        "create",
        "table",
        "varchar",
        "int",
        "view",
        "as",
        "index",
        "on",
    }

    _tok: Iterator[str]
    _current: str
    _finished: int

    def __init__(self, s: str) -> None:
        self._tok = iter(re.findall(r"\w+|\S", s.lower()))
        self._finished = False
        self._next_token()

    def match_delim(self, d: str) -> bool:
        assert len(d) == 1
        return d == self._current

    def match_int_constant(self) -> bool:
        return self._current.isnumeric()

    def match_string_constant(self) -> bool:
        return self._current == "'"

    def match_keyword(self, w: str) -> bool:
        return self._current == w

    def match_id(self) -> bool:
        return self._current not in Lexer.KEYWORDS and self._current != "'"

    def eat_delim(self, d: str) -> None:
        if not self.match_delim(d):
            raise BadSyntaxError
        self._next_token()

    def eat_int_constant(self) -> int:
        if not self.match_int_constant():
            raise BadSyntaxError
        i = int(self._current)
        self._next_token()
        return i

    def eat_string_constant(self) -> str:
        if not self.match_string_constant():
            raise BadSyntaxError
        self._next_token()
        s = self._current
        self._next_token()
        if self._current != "'":
            raise BadSyntaxError
        self._next_token()
        return s

    def eat_keyword(self, w: str) -> None:
        if not self.match_keyword(w):
            raise BadSyntaxError
        self._next_token()

    def eat_id(self) -> str:
        if not self.match_id():
            raise BadSyntaxError
        s = self._current
        self._next_token()
        return s

    def _next_token(self) -> None:
        try:
            self._current = next(self._tok)
        except StopIteration:
            if self._finished:
                raise BadSyntaxError
            else:
                self._finished = True


class ParserData:
    pass


@dataclass
class QueryData(ParserData):
    fields: Sequence[str]
    tables: Sequence[str]
    pred: Predicate

    def __str__(self) -> str:
        result = f"select {', '.join(self.fields)} from {', '.join(self.tables)}"
        if str(self.pred) != "":
            result += f" where {self.pred}"
        return result


@dataclass
class InsertData(ParserData):
    tblname: str
    flds: Sequence[str]
    vals: Sequence[Constant]


@dataclass
class DeleteData(ParserData):
    tblname: str
    pred: Predicate


@dataclass
class ModifyData(ParserData):
    tblname: str
    fldname: str
    newval: Expression
    pred: Predicate


@dataclass
class CreateTableData(ParserData):
    tblname: str
    sch: Schema


@dataclass
class CreateViewData(ParserData):
    viewname: str
    qrydata: QueryData


@dataclass
class CreateIndexData(ParserData):
    idxname: str
    tblname: str
    fldname: str


class PredParser:
    _lex: Lexer

    def __init__(self, s: str) -> None:
        self._lex = Lexer(s)

    def field(self) -> None:
        self._lex.eat_id()

    def constant(self) -> None:
        if self._lex.match_string_constant():
            self._lex.eat_string_constant()
        else:
            self._lex.eat_int_constant()

    def expression(self) -> None:
        if self._lex.match_id():
            self.field()
        else:
            self.constant()

    def term(self) -> None:
        self.expression()
        self._lex.eat_delim("=")
        self.expression()

    def predicate(self) -> None:
        self.term()
        if self._lex.match_keyword("and"):
            self._lex.eat_keyword("and")
            self.predicate()


class Parser:
    _lex: Lexer

    def __init__(self, s: str) -> None:
        self._lex = Lexer(s)

    def field(self) -> str:
        return self._lex.eat_id()

    def constant(self) -> Constant:
        if self._lex.match_string_constant():
            return Constant.from_string(self._lex.eat_string_constant())
        elif self._lex.match_int_constant():
            return Constant.from_int(self._lex.eat_int_constant())
        else:
            raise BadSyntaxError

    def expression(self) -> Expression:
        if self._lex.match_id():
            return Expression.from_field_name(self.field())
        else:
            return Expression.from_constant(self.constant())

    def term(self) -> Term:
        lhs = self.expression()
        self._lex.eat_delim("=")
        rhs = self.expression()
        return Term(lhs, rhs)

    def predicate(self) -> Predicate:
        pred = Predicate([self.term()])
        if self._lex.match_keyword("and"):
            self._lex.eat_keyword("and")
            pred.conjoin_with(self.predicate())
        return pred

    def query(self) -> QueryData:
        self._lex.eat_keyword("select")
        fields = self._select_list()
        self._lex.eat_keyword("from")
        tables = self._table_list()
        pred = Predicate()
        if self._lex.match_keyword("where"):
            self._lex.eat_keyword("where")
            pred = self.predicate()
        return QueryData(fields, tables, pred)

    def _select_list(self) -> Sequence[str]:
        L = []
        L.append(self.field())
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            L += self._select_list()
        return L

    def _table_list(self) -> Sequence[str]:
        L = []
        L.append(self._lex.eat_id())
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            L += self._table_list()
        return L

    def update_cmd(self) -> ParserData:
        if self._lex.match_keyword("insert"):
            return self.insert()
        elif self._lex.match_keyword("delete"):
            return self.delete()
        elif self._lex.match_keyword("update"):
            return self.modify()
        elif self._lex.match_keyword("create"):
            return self._create()
        else:
            raise BadSyntaxError

    def _create(self) -> ParserData:
        self._lex.eat_keyword("create")
        if self._lex.match_keyword("table"):
            return self.create_table()
        elif self._lex.match_keyword("view"):
            return self.create_view()
        elif self._lex.match_keyword("index"):
            return self.create_index()
        else:
            raise BadSyntaxError

    def delete(self) -> DeleteData:
        self._lex.eat_keyword("delete")
        self._lex.eat_keyword("from")
        tblname = self._lex.eat_id()
        pred = Predicate()
        if self._lex.match_keyword("where"):
            self._lex.eat_keyword("where")
            pred = self.predicate()
        return DeleteData(tblname, pred)

    def insert(self) -> InsertData:
        self._lex.eat_keyword("insert")
        self._lex.eat_keyword("into")
        tblname = self._lex.eat_id()
        self._lex.eat_delim("(")  # )
        flds = self._field_list()
        self._lex.eat_delim(")")
        self._lex.eat_keyword("values")
        self._lex.eat_delim("(")  # )
        vals = self._const_list()
        self._lex.eat_delim(")")
        return InsertData(tblname, flds, vals)

    def _field_list(self) -> Sequence[str]:
        L = []
        L.append(self.field())
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            L += self._field_list()
        return L

    def _const_list(self) -> Sequence[Constant]:
        L = []
        L.append(self.constant())
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            L += self._const_list()
        return L

    def modify(self) -> ModifyData:
        self._lex.eat_keyword("update")
        tblname = self._lex.eat_id()
        self._lex.eat_keyword("set")
        fldname = self.field()
        self._lex.eat_keyword("=")
        newval = self.expression()
        pred = Predicate()
        if self._lex.match_keyword("where"):
            self._lex.eat_keyword("where")
            pred = self.predicate()
        return ModifyData(tblname, fldname, newval, pred)

    def create_table(self) -> CreateTableData:
        self._lex.eat_keyword("table")
        tblname = self._lex.eat_id()
        self._lex.eat_delim("(")  # )
        sch = self._field_defs()
        self._lex.eat_delim(")")
        return CreateTableData(tblname, sch)

    def _field_defs(self) -> Schema:
        schema = self._field_def()
        if self._lex.match_delim(","):
            self._lex.eat_delim(",")
            schema2 = self._field_defs()
            schema.add_all(schema2)
        return schema

    def _field_def(self) -> Schema:
        fldname = self.field()
        return self._field_type(fldname)

    def _field_type(self, fldname: str) -> Schema:
        schema = Schema()
        if self._lex.match_keyword("int"):
            self._lex.eat_keyword("int")
            schema.add_int_field(fldname)
        elif self._lex.match_keyword("varchar"):
            self._lex.eat_keyword("varchar")
            self._lex.eat_delim("(")  # )
            str_len = self._lex.eat_int_constant()
            self._lex.eat_delim(")")
            schema.add_string_field(fldname, str_len)
        else:
            raise BadSyntaxError
        return schema

    def create_view(self) -> CreateViewData:
        self._lex.eat_keyword("view")
        viewname = self._lex.eat_id()
        self._lex.eat_keyword("as")
        qd = self.query()
        return CreateViewData(viewname, qd)

    def create_index(self) -> CreateIndexData:
        self._lex.eat_keyword("index")
        idxname = self._lex.eat_id()
        self._lex.eat_keyword("on")
        tblname = self._lex.eat_id()
        self._lex.eat_delim("(")  # )
        fldname = self.field()
        self._lex.eat_delim(")")
        return CreateIndexData(idxname, tblname, fldname)
