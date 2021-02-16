"""Simple object manager."""

import abc
import importlib
import weakref

from notanorm import DbBase, SqliteDb, errors as err, DbType, DbModel
from typing import Any, Optional, Dict, Type, Set

from omen2.errors import OmenMoreThanOneError
from omen2.object import ObjBase, ObjType


class RowIter:
    def __init__(self, itr):
        self.__iter = itr
        self.__first = None
        self.__bool = None

    def __len__(self):
        cnt = 0
        for _ in self:
            cnt += 1
        return cnt

    def __bool__(self):
        if self.__bool is None:
            try:
                self.__first = next(self.__iter)
                self.__bool = True
            except (StopIteration, err.TableNotFoundError):
                self.__bool = False
        return self.__bool

    def __iter__(self):
        return self

    def __next__(self):
        if self.__first is not None:
            self.__first = None
            return self.__first
        return next(self.__iter)

def default_type(typ: DbType):
    if typ == DbType.ANY:
        return Any
    if typ == DbType.INTEGER:
        return int
    if typ == DbType.FLOAT:
        return float
    if typ == DbType.TEXT:
        return str
    if typ == DbType.BLOB:
        return bytes
    if typ == DbType.DOUBLE:
        return float


# noinspection PyMethodMayBeStatic
class Omen(abc.ABC):
    """Object relational manager: read and write objects from a db."""

    version: Optional[int] = None
    model: DbModel = None
    table_types: Dict[str, Type['Table']] = {}

    def __init_subclass__(cls):
        if not cls.model:
            db = SqliteDb(":memory:")
            cls.__multi_query(db, cls.schema(cls.version))
            cls.model: DbModel = db.model()

    def __init__(self, db: DbBase, migrate=True, codegen=True, **table_types):
        """Create a new manager with a db connection."""
        self.db = db

        if migrate:
            self._migrate_if_needed()
        self._create_if_needed()

        self.table_types = table_types

        if codegen:
            self.codegen_support()

        self.validate_model()

        # create table containers
        for name, table_type in self.table_types.items():
            setattr(self, name, table_type(self))

    def validate_model(self):
        # codegen should be optional, so validate that the models match up
        for name, tab in self.table_types.items():
            assert issubclass(tab, Table)
            assert tab.table_name == name
            assert issubclass(tab.row_type, ObjBase)
            assert getattr(tab.row_type, "_pk")
            assert tab.row_type.table_type is tab

    def codegen_support(self):
        module = self.__class__.__module__ + "_gen"
        try:
            generated = importlib.import_module(module)
        except ImportError:
            from .codegen import CodeGen
            CodeGen.generate_from_class(self.__class__)
            generated = importlib.import_module(module)

        for name in generated.__all__:
            table_type = getattr(generated, name)
            if name not in self.table_types:
                self.table_types[name] = table_type

    @staticmethod
    def __multi_query(db, sql):
        unlikely = "@!~@"
        sql = sql.replace("\\;", unlikely)
        queries = sql.split(";")
        for q in queries:
            q = q.replace(unlikely, ";")
            db.query(q)

    def _create_if_needed(self):
        if not self.db.model() == self.model:
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
        ...

    def migrate(self, db, version):
        raise NotImplementedError

    def restore(self, backup_info: Any):
        raise NotImplementedError

    def backup(self) -> Any:
        return object()


# noinspection PyDefaultArgument,PyProtectedMember
class Table:
    table_name: str
    row_type: ObjType
    field_names: Set[str]

    def __init_subclass__(cls, **kwargs):
        cls.row_type.table_type = cls

    def __init__(self, mgr):
        self.manager = mgr
        self.__cache = weakref.WeakValueDictionary()

    @property
    def db(self):
        return self.manager.db

    def add(self, obj):
        """Insert an object into the db, object must support to_dict()."""
        self.__cache[obj._to_pk_tuple()] = obj
        obj._bind(table=self)
        obj._commit()
        return obj

    def remove(self, obj: ObjBase):
        """Insert an object into the db, object must support to_dict()."""
        self.__cache.pop(obj._to_pk_tuple(), None)
        vals = obj.to_pk()
        self.db.delete(**vals)

    def update(self, obj: "ObjBase"):
        """Update the db from an object, object must support to_dict()."""
        self.__cache[obj._to_pk_tuple()] = obj
        obj._bind(table=self)
        vals = obj.to_dict()
        need_id_field = None
        for field in obj._pk:
            if getattr(obj, field, None) is None:
                assert not need_id_field
                need_id_field = field
        if need_id_field:
            ret = self.db.insert(self.table_name, **vals)
            with obj:
                setattr(obj, need_id_field, ret.lastrowid)
        else:
            self.db.upsert(self.table_name, **vals)

    def __select(self, where):
        for row in self.db.select(self.table_name, None, where):
            obj = self.row_type.from_db(row.__dict__)
            obj = self.__cache.get(obj._to_pk_tuple(), obj)
            obj._bind(table=self)
            yield obj

    def select(self, where={}, **kws):
        """Read objects of specified class."""
        kws.update(where)
        yield from self.__select(kws)

    def count(self, where={}, **kws):
        """Return count of objs matchig where clause."""
        kws.update(where)
        return self.db.count(self.table_name, kws)

    def select_one(self, where={}, **kws):
        """Return one row, None, or raises an OmenMoreThanOneError."""
        itr = self.select(where, **kws)
        try:
            one = next(itr)
        except StopIteration:
            return None

        try:
            next(itr)
            raise OmenMoreThanOneError
        except StopIteration:
            return one
