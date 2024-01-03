from abc import abstractmethod
from functools import reduce
from typing import Sequence

from simpledbpy.metadata import MetadataMgr, StatInfo
from simpledbpy.parser import (
    CreateIndexData,
    CreateTableData,
    CreateViewData,
    DeleteData,
    InsertData,
    ModifyData,
    Parser,
    QueryData,
)
from simpledbpy.record import Layout, Schema

from simpledbpy.scan import (
    Predicate,
    ProductScan,
    ProjectScan,
    Scan,
    SelectScan,
    TableScan,
    UpdateScan,
)
from simpledbpy.transaction import Transaction


class Plan:
    @abstractmethod
    def open(self) -> Scan:
        raise NotImplementedError

    @abstractmethod
    def block_accessed(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def records_output(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def distinct_values(self, fldname: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def schema(self) -> Schema:
        raise NotImplementedError


class TablePlan(Plan):
    _tx: Transaction
    _tblname: str
    _layout: Layout
    _si: StatInfo

    def __init__(self, tx: Transaction, tblname: str, md: MetadataMgr) -> None:
        self._tx = tx
        self._tblname = tblname
        self._layout = md.get_layout(tblname, tx)
        self._si = md.get_stat_info(tblname, self._layout, tx)

    def open(self) -> Scan:
        return TableScan(self._tx, self._tblname, self._layout)

    def block_accessed(self) -> int:
        return self._si.num_blocks

    def records_output(self) -> int:
        return self._si.num_recs

    def distinct_values(self, fldname: str) -> int:
        return self._si.distinct_values(fldname)

    def schema(self) -> Schema:
        return self._layout.schema()


class SelectPlan(Plan):
    _p: Plan
    _pred: Predicate

    def __init__(self, p: Plan, pred: Predicate) -> None:
        self._p = p
        self._pred = pred

    def open(self) -> Scan:
        s = self._p.open()
        return SelectScan(s, self._pred)

    def block_accessed(self) -> int:
        return self._p.block_accessed()

    def records_output(self) -> int:
        return self._p.records_output() // self._pred.reduction_factor(self._p)

    def distinct_values(self, fldname: str) -> int:
        if self._pred.equates_with_constant(fldname) is not None:
            return 1
        else:
            fldname2 = self._pred.equates_with_field(fldname)
            if fldname2 is not None:
                return min(
                    self._p.distinct_values(fldname), self._p.distinct_values(fldname2)
                )
            else:
                return self._p.distinct_values(fldname)

    def schema(self) -> Schema:
        return self._p.schema()


class ProjectPlan(Plan):
    _p: Plan
    _schema: Schema

    def __init__(self, p: Plan, fieldlist: Sequence[str]) -> None:
        self._p = p
        self._schema = Schema()
        for fldname in fieldlist:
            self._schema.add(fldname, self._p.schema())

    def open(self) -> Scan:
        s = self._p.open()
        return ProjectScan(s, self._schema.fields())

    def block_accessed(self) -> int:
        return self._p.block_accessed()

    def records_output(self) -> int:
        return self._p.records_output()

    def distinct_values(self, fldname: str) -> int:
        return self._p.distinct_values(fldname)

    def schema(self) -> Schema:
        return self._schema


class ProductPlan(Plan):
    _p1: Plan
    _p2: Plan
    _schema: Schema

    def __init__(self, p1: Plan, p2: Plan) -> None:
        self._p1 = p1
        self._p2 = p2
        self._schema = Schema()
        self._schema.add_all(p1.schema())
        self._schema.add_all(p2.schema())

    def open(self) -> Scan:
        s1 = self._p1.open()
        s2 = self._p2.open()
        return ProductScan(s1, s2)

    def block_accessed(self) -> int:
        return (
            self._p1.block_accessed()
            + self._p1.records_output() * self._p2.block_accessed()
        )

    def records_output(self) -> int:
        return self._p1.records_output() * self._p2.records_output()

    def distinct_values(self, fldname: str) -> int:
        if self._p1.schema().has_field(fldname):
            return self._p1.distinct_values(fldname)
        else:
            return self._p2.distinct_values(fldname)

    def schema(self) -> Schema:
        return self._schema


class QueryPlanner:
    @abstractmethod
    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        raise NotImplementedError


class BasicQueryPlanner(QueryPlanner):
    _mdm: MetadataMgr

    def __init__(self, mdm: MetadataMgr) -> None:
        self._mdm = mdm

    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        plans = []
        for tblname in data.tables:
            viewdef = self._mdm.get_view_def(tblname, tx)
            if viewdef is not None:
                parser = Parser(viewdef)
                viewdata = parser.query()
                plans.append(self.create_plan(viewdata, tx))
            else:
                plans.append(TablePlan(tx, tblname, self._mdm))
        p = reduce(lambda p1, p2: ProductPlan(p1, p2), plans[1:], plans[0])
        p = SelectPlan(p, data.pred)
        return ProjectPlan(p, data.fields)


class BetterQueryPlanner(QueryPlanner):
    _mdm: MetadataMgr

    def __init__(self, mdm: MetadataMgr) -> None:
        self._mdm = mdm

    def create_plan(self, data: QueryData, tx: Transaction) -> Plan:
        plans = []
        for tblname in data.tables:
            viewdef = self._mdm.get_view_def(tblname, tx)
            if viewdef is not None:
                parser = Parser(viewdef)
                viewdata = parser.query()
                plans.append(self.create_plan(viewdata, tx))
            else:
                plans.append(TablePlan(tx, tblname, self._mdm))
        p = plans[0]
        for nextplan in plans[1:]:
            p1 = ProductPlan(nextplan, p)
            p2 = ProductPlan(p, nextplan)
            p = p1 if p1.block_accessed() < p2.block_accessed() else p2
        p = SelectPlan(p, data.pred)
        return ProjectPlan(p, data.fields)


class UpdatePlanner:
    @abstractmethod
    def execute_insert(self, data: InsertData, tx: Transaction) -> int:
        raise NotImplementedError

    @abstractmethod
    def execute_delete(self, data: DeleteData, tx: Transaction) -> int:
        raise NotImplementedError

    @abstractmethod
    def execute_modify(self, data: ModifyData, tx: Transaction) -> int:
        raise NotImplementedError

    @abstractmethod
    def execute_create_table(self, data: CreateTableData, tx: Transaction) -> int:
        raise NotImplementedError

    @abstractmethod
    def execute_create_view(self, data: CreateViewData, tx: Transaction) -> int:
        raise NotImplementedError

    @abstractmethod
    def execute_create_index(self, data: CreateIndexData, tx: Transaction) -> int:
        raise NotImplementedError


class BasicUpdatePlanner(UpdatePlanner):
    _mdm: MetadataMgr

    def __init__(self, mdm: MetadataMgr) -> None:
        self._mdm = mdm

    def execute_insert(self, data: InsertData, tx: Transaction) -> int:
        p = TablePlan(tx, data.tblname, self._mdm)
        us = p.open()
        assert isinstance(us, UpdateScan)
        us.insert()
        for fldname, val in zip(data.flds, data.vals):
            us.set_val(fldname, val)
        us.close()
        return 1

    def execute_delete(self, data: DeleteData, tx: Transaction) -> int:
        p: Plan = TablePlan(tx, data.tblname, self._mdm)
        p = SelectPlan(p, data.pred)
        us = p.open()
        assert isinstance(us, UpdateScan)
        count = 0
        while us.next():
            us.delete()
            count += 1
        us.close()
        return count

    def execute_modify(self, data: ModifyData, tx: Transaction) -> int:
        p: Plan = TablePlan(tx, data.tblname, self._mdm)
        p = SelectPlan(p, data.pred)
        us = p.open()
        assert isinstance(us, UpdateScan)
        count = 0
        while us.next():
            val = data.newval.evaluate(us)
            us.set_val(data.fldname, val)
            count += 1
        us.close()
        return count

    def execute_create_table(self, data: CreateTableData, tx: Transaction) -> int:
        self._mdm.create_table(data.tblname, data.sch, tx)
        return 0

    def execute_create_view(self, data: CreateViewData, tx: Transaction) -> int:
        self._mdm.create_view(data.viewname, str(data.qrydata), tx)
        return 0

    def execute_create_index(self, data: CreateIndexData, tx: Transaction) -> int:
        self._mdm.create_index(data.idxname, data.tblname, data.fldname, tx)
        return 0


class Planner:
    _qplanner: QueryPlanner
    _uplanner: UpdatePlanner

    def __init__(self, qplanner: QueryPlanner, uplanner: UpdatePlanner) -> None:
        self._qplanner = qplanner
        self._uplanner = uplanner

    def create_query_plan(self, cmd: str, tx: Transaction) -> Plan:
        parser = Parser(cmd)
        data = parser.query()
        return self._qplanner.create_plan(data, tx)

    def execute_update(self, cmd: str, tx: Transaction) -> int:
        parser = Parser(cmd)
        obj = parser.update_cmd()
        if isinstance(obj, InsertData):
            return self._uplanner.execute_insert(obj, tx)
        elif isinstance(obj, DeleteData):
            return self._uplanner.execute_delete(obj, tx)
        elif isinstance(obj, ModifyData):
            return self._uplanner.execute_modify(obj, tx)
        elif isinstance(obj, CreateTableData):
            return self._uplanner.execute_create_table(obj, tx)
        elif isinstance(obj, CreateViewData):
            return self._uplanner.execute_create_view(obj, tx)
        elif isinstance(obj, CreateIndexData):
            return self._uplanner.execute_create_index(obj, tx)
        else:
            return 0
