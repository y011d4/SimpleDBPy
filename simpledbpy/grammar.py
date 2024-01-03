from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING
from simpledbpy.record import Schema

if TYPE_CHECKING:
    from simpledbpy.scan import Scan
    from simpledbpy.plan import Plan


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

    def reduction_factor(self, p: Plan) -> int:
        if self.lhs.is_field_name() and self.rhs.is_field_name():
            lhs_name = self.lhs.fldname
            rhs_name = self.rhs.fldname
            assert lhs_name is not None
            assert rhs_name is not None
            return max(p.distinct_values(lhs_name), p.distinct_values(rhs_name))
        if self.lhs.is_field_name():
            lhs_name = self.lhs.fldname
            assert lhs_name is not None
            return p.distinct_values(lhs_name)
        if self.rhs.is_field_name():
            rhs_name = self.rhs.fldname
            assert rhs_name is not None
            return p.distinct_values(rhs_name)
        if self.lhs.val == self.rhs.val:
            return 1
        else:
            return 2**31 - 1

    def equates_with_constant(self, fldname: str) -> Optional[Constant]:
        if (
            self.lhs.is_field_name()
            and self.lhs.fldname == fldname
            and not self.rhs.is_field_name()
        ):
            return self.rhs.val
        elif (
            self.rhs.is_field_name()
            and self.rhs.fldname == fldname
            and not self.lhs.is_field_name()
        ):
            return self.lhs.val
        else:
            return None

    def equates_with_field(self, fldname: str) -> Optional[str]:
        if (
            self.lhs.is_field_name()
            and self.lhs.fldname == fldname
            and self.rhs.is_field_name()
        ):
            return self.rhs.fldname
        elif (
            self.rhs.is_field_name()
            and self.rhs.fldname == fldname
            and self.lhs.is_field_name()
        ):
            return self.lhs.fldname
        else:
            return None

    def __str__(self) -> str:
        return f"{self.lhs}={self.rhs}"
