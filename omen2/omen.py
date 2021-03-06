"""Simple object manager."""

import abc
import importlib
import os
import sys
import logging as log

from notanorm import DbBase, SqliteDb, DbType, DbModel
from typing import Any, Optional, Dict, Type, Iterable, TypeVar

from .table import Table
from .object import ObjBase
from .codegen import CodeGen

T = TypeVar("T", bound=Table)


def any_type(arg):
    return arg


def default_type(typ: DbType) -> Any:  # pylint: disable=too-many-return-statements
    if typ == DbType.ANY:
        return any_type
    if typ == DbType.INTEGER:
        return int
    if typ == DbType.FLOAT:
        return float
    if typ == DbType.TEXT:
        return str
    if typ == DbType.BLOB:
        return bytes
    if typ == DbType.BOOLEAN:
        return bool
    if typ == DbType.DOUBLE:
        return float
    raise ValueError("unknown type: %s" % typ)


# noinspection PyMethodMayBeStatic,PyProtectedMember
class Omen(abc.ABC):
    """Object relational manager: read and write objects from a db."""

    # pylint: disable=protected-access

    # abstract classes often do this, so # pylint: disable=no-self-use

    generate_code = True
    version: Optional[int] = None
    model: DbModel = None
    table_types: Dict[str, Type["Table"]] = None
    tables: Dict[Type["Table"], "Table"] = None

    def __init_subclass__(cls, **_kws):
        cls.table_types = {}

        if not cls.model:
            db = SqliteDb(":memory:")
            cls.__multi_query(db, cls.schema(cls.version))
            cls.model: DbModel = db.model()

    def __init__(self, db: DbBase, migrate=True, **table_types):
        """Create a new manager with a db connection."""
        self.tables = {}
        self.db = db

        if migrate:
            self._migrate_if_needed()
        self._create_if_needed()

        self.table_types.update(table_types)

        for name, table_type in self.table_types.items():
            # allow user to specify the table name this way instead
            if not hasattr(table_type, "table_name"):
                table_type.table_name = name
            table_type(self)

    def get_table_by_name(self, table_name):
        return self[self.table_types[table_name]]

    def __getitem__(self, table_type: Type[T]) -> T:
        return self.tables[table_type]

    def __setitem__(self, table_type: Type[T], table: T):
        assert table_type.table_name == table.table_name
        self.table_types[table.table_name] = table_type
        self.validate_table(table.table_name, table_type)
        self.tables[table_type] = table

    def set_table(self, table: Table):
        self[type(table)] = table

    def load_dict(self, data_set: Dict[str, Iterable[Dict[str, Any]]]):
        # load sample data into self
        for name, values in data_set.items():
            tab: Table = self.get_table_by_name(name)
            for entry in values:
                tab.add(tab.row_type._from_db(entry))

    def dump_dict(self) -> Dict[str, Iterable[Dict[str, Any]]]:
        ret = {}
        for ttype, tab in self.tables.items():
            lst = []
            for obj in tab:
                lst.append(obj._to_db())
            ret[ttype.table_name] = lst
        return ret

    def validate_table(self, name, tab):
        assert issubclass(tab, Table)

        if not getattr(tab, "table_name", None):
            tab.table_name = name
        assert tab.table_name == name

        assert issubclass(tab.row_type, ObjBase)
        assert isinstance(getattr(tab.row_type, "_pk"), tuple)
        assert issubclass(tab.row_type._table_type, tab)

        if not getattr(tab, "field_names", None):
            log.debug("%s: default serialization field names used", name)
            tab.field_names = {c.name for c in self.model[name].columns}
        assert isinstance(tab.field_names, set)

        pk = None
        model = self.model[name]
        for idx in model.indexes:
            if idx.primary:
                pk = idx.fields
        if len(pk) == 1:
            for fd in model.columns:
                if fd.name == pk[0]:
                    tab.allow_auto = fd.autoinc

    @classmethod
    def codegen(cls, force=False):
        """Generate code derived from my model, and put it next to my __file__."""
        if cls.__module__ == "__main__":
            module, _ = os.path.splitext(
                os.path.basename(sys.modules["__main__"].__file__)
            )
        else:
            module = cls.__module__

        module += "_gen"

        try:
            if force:
                raise ImportError
            generated = importlib.import_module(module)
        except (ImportError, SyntaxError):
            CodeGen.generate_from_class(cls)
            generated = importlib.import_module(module)

        for name in generated.__all__:
            table_type = getattr(generated, name)
            if name not in cls.table_types:
                cls.table_types[name] = table_type

    @staticmethod
    def __multi_query(db, sql):
        unlikely = "@!~@"
        sql = sql.replace("\\;", unlikely)
        queries = sql.split(";")
        for q in queries:
            q = q.replace(unlikely, ";")
            db.query(q)

    def _create_if_needed(self):
        mod1 = self.db.model()
        mod2 = self.model
        mod1.pop("_omen", None)
        mod2.pop("_omen", None)
        if not mod1 == mod2:
            self.__multi_query(self.db, self.schema(self.version))

    def _migrate_if_needed(self):
        if self.version is None:
            return

        # omen, built-in version management
        self.db.query("create table if not exists _omen(version text);")
        omen_info = self.db.select_one("_omen")

        if omen_info:
            restore_info = self.backup()
            try:
                next_version = omen_info.version
                while omen_info.version != self.version:
                    next_version += 1
                    self.migrate(self.db, next_version)
                    self.db.upsert_all("_omen", version=next_version)
            finally:
                self.restore(restore_info)

    @classmethod
    @abc.abstractmethod
    def schema(cls, version):
        """Override this to return a schema for a given version."""
        ...

    def migrate(self, db, version):
        """Override this to support migration."""
        raise NotImplementedError

    def restore(self, backup_info: Any):
        """Override this to support backup and recovery during migration."""
        raise NotImplementedError

    def backup(self) -> Any:
        """Override this to support backup and recovery during migration."""
        return object()
