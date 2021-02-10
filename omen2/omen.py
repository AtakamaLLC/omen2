"""Simple object manager."""

import abc
from notanorm import DbBase, SqliteDb, errors as err, DbType, DbTable
from typing import Any

class OmenError(RuntimeError):
    pass


class OmenMoreThanOneError(OmenError):
    pass


class RowIter:
    def __init__(self, itr):
        self.__iter = itr
        self.__first = None
        self.__bool = None

    def __bool__(self):
        if self.__bool is None:
            try:
                self.__first = next(self.__iter)
                self.__bool = True
            except (StopIteration, err.TableNotFoundError):
                self.__bool = False
        return self.__bool

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


class Omen(abc.ABC):
    """Object relational manager: read and write objects from a db."""

    version = None
    model = None 

    def __init__(self, db: DbBase, migrate=True):
        """Create a new manager with a db connection."""
        self.db = db

        if self.version is not None:
            self._create_and_migrate(migrate)

    @staticmethod
    def __multi_query(db, sql):
        unlikely = "@!~@"
        sql = sql.replace("\\;", unlikely)
        queries = sql.split(";")
        for q in queries:
            q = q.replace(unlikely, ";")
            db.query(q)

    def _create_and_migrate(self, migrate):
        # omen, built-in version management
        self.db.query("create table if not exists _omen(version text);")
        omen_info = self.db.select_one("_omen")

        if not omen_info:
            self.__multi_query(self.db, self.schema(self.version))
            self.db.upsert("_omen", version=self.version)

        if migrate:
            restore_info = self.backup()
            try:
                next_version = omen_info.version
                while omen_info.version != self.version:
                    next_version += 1
                    self.migrate(self.db, next_version)
                self.db.upsert("_omen", version=self.version)
            finally:
                self.restore(restore_info)

    def __init_subclass__(cls):
        if not cls.model:
            db = SqliteDb(":memory:")
            cls.__multi_query(db, cls.schema(cls.version))
            cls.model = db.model()

            # creates a new type, derived from Table, with attributes matchiing the columns
            for table, info in cls.model.items():
                setattr(cls, table, type(table, (Table, ), {}))
                getattr(cls, table).set_schema(info)

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


class Table:
    @classmethod
    def set_schema(cls, schema: DbTable):
        setattr(cls, "__annotations__", getattr(cls, "__annotations__", {}))
        for col in schema.columns:
            typ = default_type(col.typ)
            cls.__annotations__[col.name] = typ
            if col.default:
                setattr(cls, col.name, typ(col.default))

    def __init__(self, mgr):
        self.__manager__ = mgr

    @property
    def db(self):
        return self.__manager__.db

    def insert(self, obj):
        """Insert an object into the db, object must support to_dict()."""
        vals = obj.to_dict()
        self.db.insert(obj.__table_name__, vals)

    def update(self, obj):
        """Update the db from an object, object must support to_dict()."""
        vals = obj.to_dict()
        self.db.update(obj.__table_name__, vals)

    def __select(self, cls, where):
        for row in self.db.select(cls.__table_name__, None, where):
            yield cls.from_dict(row.__dict__)

    def select(self, cls, where={}, **kws):
        """Read objects of specified class."""
        kws.update(where)
        return RowIter(self.__select(cls, kws))

    def count(self, cls, where={}, **kws):
        """Return count of objs matchig where clause."""
        kws.update(where)
        return self.db.count(cls.__table_name__, kws)

    def select_one(self, cls, where={}, **kws):
        """Return one row, None, or raises an OmenMoreThanOneError."""
        itr = self.select(cls, where, **kws)
        try:
            one = iter(itr)
        except StopIteration:
            return None

        try:
            iter(itr)
            raise OmenMoreThanOneError
        except StopIteration:
            return one
