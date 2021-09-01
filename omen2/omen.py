"""Simple object manager."""

import abc
import importlib
import os
import sys
import logging as log

from notanorm import DbBase, SqliteDb, DbModel
from typing import Any, Optional, Dict, Type, Iterable, TypeVar

from .table import Table
from .object import ObjBase
from .codegen import CodeGen

T = TypeVar("T", bound=Table)


# noinspection PyMethodMayBeStatic,PyProtectedMember
class Omen(abc.ABC):
    """Object relational manager: read and write objects from a db."""

    # pylint: disable=protected-access

    # abstract classes often do this, so # pylint: disable=no-self-use

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

    def __init__(self, db: DbBase, module=None, **table_types):
        """Create a new manager with a db connection."""
        self.tables = {}
        self.db = db

        self._create_if_needed()

        self.table_types.update(table_types)

        if module:
            self._init_module(module)

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
        self._validate_table(table.table_name, table_type)
        self.tables[table_type] = table

    def set_table(self, table: Table):
        self[type(table)] = table

    def load_dict(self, data_set: Dict[str, Iterable[Dict[str, Any]]]):
        """Load every table from a dictionary."""
        # load sample data into self
        for name, values in data_set.items():
            tab: Table = self.get_table_by_name(name)
            for entry in values:
                tab.add(tab.row_type._from_db(entry))

    def dump_dict(self) -> Dict[str, Iterable[Dict[str, Any]]]:
        """Dump every table as a dictionary.

        This just loops through all objects and calls _to_db on them.
        """
        ret = {}
        for ttype, tab in self.tables.items():
            lst = []
            for obj in tab:
                lst.append(obj._to_db())
            ret[ttype.table_name] = lst
        return ret

    def _validate_table(self, name, tab):
        """Check if the class defined by tab is a valid table.

        This will also attempt to update these class variables, if they are not set:
            - row_type will be inferred from type vars, if any
            - field_names will be inferred from the table_name
            - allow_auto will be inferred from the primary key
        """
        assert issubclass(tab, Table)

        if not getattr(tab, "table_name", None):
            tab.table_name = name
        assert tab.table_name == name

        if not hasattr(tab, "row_type"):
            self._bootstrap_row_type(tab)

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
        if len(pk) == 1 and tab.allow_auto is None:
            for fd in model.columns:
                if fd.name == pk[0]:
                    tab.allow_auto = fd.autoinc

    def _bootstrap_row_type(self, tab):
        """Given a class, figure guess the row_type based on the type hint.

        For example:

        class Cars(Table[Car]):
            pass

        row_type will be set to Car
        """
        bases = getattr(tab, "__orig_bases__", None)
        if bases:
            args = getattr(bases[0], "__args__")
            if args and issubclass(args[0], ObjBase):
                tab.row_type = args[0]
                tab.row_type._table_type = tab

    @classmethod
    def codegen(cls, only_if_missing=False):
        """Generate code derived from my model, and put it next to my __file__."""
        if cls.__module__ == "__main__":
            module, _ = os.path.splitext(
                os.path.basename(sys.modules["__main__"].__file__)
            )
        else:
            module = cls.__module__

        module += "_gen"

        try:
            if not only_if_missing:
                raise ImportError
            generated = importlib.import_module(module)
        except (ImportError, SyntaxError):
            generated = CodeGen.generate_from_class(cls)

        cls._init_module(generated)

        return generated

    @classmethod
    def _init_module(cls, module):
        for name in getattr(module, "__all__", dir(module)):
            table_type = getattr(module, name)
            if isinstance(table_type, type) and issubclass(table_type, Table):
                cls.table_types[table_type.table_name] = table_type

    @staticmethod
    def __multi_query(db, sql):
        unlikely = "@!'\"~z@"
        assert unlikely not in sql
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

    @classmethod
    @abc.abstractmethod
    def schema(cls, version):
        """Override this to return a schema for a given version."""
        ...
