# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Simple object manager."""

import abc
import contextlib
import importlib
import os
import sys
import logging as log
from contextlib import contextmanager

from notanorm import DbBase, DbModel, model_from_ddl
from typing import Any, Optional, Dict, Type, Iterable, TypeVar

from .table import Table
from .object import ObjBase
from .codegen import CodeGen
from .errors import OmenRollbackError

T = TypeVar("T", bound=Table)


# noinspection PyMethodMayBeStatic,PyProtectedMember
class Omen(abc.ABC):
    """Object relational manager: read and write objects from a db."""

    # pylint: disable=protected-access

    version: Optional[int] = None
    model: DbModel = None
    table_types: Dict[str, Type["Table"]] = None

    # todo: deprecate this
    AUTOCREATE = True
    AUTOCREATE_IGNORE_TABLES = ["_omen"]

    def __init_subclass__(cls, **_kws):
        cls.table_types = {}

        if not cls.model:
            ddl = cls.schema(cls.version)
            dialect = getattr(cls, "dialect", None)
            dialect = (dialect,) if dialect else ()
            cls.model: DbModel = model_from_ddl(ddl, *dialect)

    def __init__(self, db: DbBase, module=None, type_checking=False, **table_types):
        """Create a new manager with a db connection."""
        # if you initialize two instances with different table types, each will use its own
        self.table_types = self.table_types.copy()
        self.tables: Dict[Type["Table"], "Table"] = {}
        self.db = db

        if self.AUTOCREATE:
            self._create_if_needed()

        self.table_types.update(table_types)

        if module:
            self._init_module(module, self.table_types)

        for name, table_type in self.table_types.items():
            # allow user to specify the table name this way instead
            if not hasattr(table_type, "table_name"):
                table_type.table_name = name
            if getattr(table_type, "_type_check", None) is None:
                table_type._type_check = type_checking
            table_type(self)

    def get_table_by_name(self, table_name):
        """Get table object by table name."""
        return self[self.table_types[table_name]]

    def __getitem__(self, table_type: Type[T]) -> T:
        """Get table object by table type."""
        return self.tables[table_type]

    def __setitem__(self, table_type: Type[T], table: T):
        """Set the table object associated with the table type."""
        assert table_type.table_name == table.table_name
        self.table_types[table.table_name] = table_type
        self._validate_table(table.table_name, table_type)
        self.tables[table_type] = table

    def set_table(self, table: Table):
        """Set the table object associated with teh table type"""
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

        if pk and len(pk) == 1 and tab.allow_auto is None:
            for fd in model.columns:
                if fd.name == (pk[0] if type(pk[0]) is str else pk[0].name):
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
    def codegen(cls, only_if_missing=False, out_path=None):
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
            generated = CodeGen.generate_from_class(cls, out_path=out_path)

        cls._init_module(generated, cls.table_types)

        return generated

    @classmethod
    def _init_module(cls, module, table_types):
        for name in getattr(module, "__all__", dir(module)):
            table_type = getattr(module, name)
            if isinstance(table_type, type) and issubclass(table_type, Table):
                table_types[table_type.table_name] = table_type

    def _create_if_needed(self):
        # TODO: this should be removed, not good behavior
        mod1 = self.db.model()
        mod2 = self.model
        for tab in self.AUTOCREATE_IGNORE_TABLES:
            mod1.pop(tab, None)
            mod2.pop(tab, None)
        if not sorted(mod1.keys()) == sorted(mod2.keys()):
            # create missing tables
            mod = DbModel({k: v for k, v in self.model.items() if k not in mod1})
            self.db.create_model(mod)

    @classmethod
    @abc.abstractmethod
    def schema(cls, version):
        """Override this to return a schema for a given version."""

    @contextmanager
    def transaction(self):
        """Begin a database-wide transaction.

        This will accumulate object modifications, adds and removes, and roll them back on exception.

        It uses the underlying database's transaction mechanism.

        On exception it will restore any cached information to the previous state.
        """
        try:
            with contextlib.ExitStack() as stack:
                for tab in self.tables.values():
                    stack.enter_context(tab._unsafe_transaction())
                yield self
        except OmenRollbackError:
            pass
