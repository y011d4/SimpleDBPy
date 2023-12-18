from dataclasses import dataclass
from typing import Optional
from simpledbpy.record import Schema

from simpledbpy.scan import Scan


@dataclass
class Constant:
    ival: Optional[int]
    sval: Optional[str]

    @classmethod
    def from_int(cls, ival: int) -> "Constant":
        return Constant(ival=ival, sval=None)

    @classmethod
    def from_string(cls, sval: str) -> "Constant":
        return Constant(ival=None, sval=sval)

    def __str__(self) -> str:
        if self.ival is None and self.sval is not None:
            return self.sval
        elif self.ival is not None and self.sval is None:
            return str(self.ival)
        else:
            raise RuntimeError


@dataclass
class Expression:
    val: Optional[Constant]
    fldname: Optional[str]

    @classmethod
    def from_constant(cls, val: Constant) -> "Expression":
        return Expression(val=val, fldname=None)

    @classmethod
    def from_field_name(cls, fldname: str) -> "Expression":
        return Expression(val=None, fldname=fldname)

    def is_field_name(self) -> bool:
        return self.fldname is not None

    def evaluate(self, s: Scan) -> Constant:
        if self.val is None and self.fldname is not None:
            return s.get_val(self.fldname)
        elif self.val is not None and self.fldname is None:
            return self.val
        else:
            raise RuntimeError

    def applies_to(self, sch: Schema) -> bool:
        if self.val is None and self.fldname is not None:
            return sch.has_field(self.fldname)
        elif self.val is not None and self.fldname is None:
            return True
        else:
            raise RuntimeError

    def __str__(self) -> str:
        if self.val is None and self.fldname is not None:
            return self.fldname
        elif self.val is not None and self.fldname is None:
            return str(self.val)
        else:
            raise RuntimeError


@dataclass
class Term:
    lhs: Expression
    rhs: Expression

    def is_satisfied(self, s: Scan):
        lhsval = self.lhs.evaluate(s)
        rhsval = self.rhs.evaluate(s)
        return rhsval == lhsval

    def applies_to(self, sch: Schema) -> bool:
        return self.lhs.applies_to(sch) and self.rhs.applies_to(sch)

    # def reduction_factor(self, p: Plan) -> int:
    #     pass

    # def equates_with_constant(self, fldname: str) -> Constant:
    #     pass

    # def equates_with_field(self, fldname: str) -> str:
    #     pass

    def __str__(self) -> str:
        return f"{self.lhs}={self.rhs}"
