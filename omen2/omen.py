"""Simple orm object manager."""

import abc
from notanorm import DbBase, SqliteDb, errors as err
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


class Omen(abc.ABC):
    """Object relational manager: read and write objects from a db."""

    version = None
    model = None 

    def __init__(self, db: DbBase, migrate=True):
        """Create a new manager with a db connection."""
        self.db = db

        if self.version is not None:
            # omen, built-in version management
            self.db.query("create table if not exists _omen(version text);")
            omen_info = self.db.select_one("_omen")

            if not omen_info:
                self.db.query(self.schema(self.version))
                self.db.upsert("_omen", version=self.version)

            if migrate:
                restore_info = self.backup()
                try:
                    next_version = omen_info.version
                    while omen_info.version != self.version:
                        next_version += 1
                        self.migrate(db, next_version)
                    self.db.upsert("_omen", version=self.version)
                finally:
                    self.restore(restore_info)

    def __init_subclass__(cls):
        if not cls.model:
            cls.model = SqliteDb(":memory:")
            cls.model.query(cls.schema(cls.version))

            # creates a new type, derived from Table, with attributes matchiing the columns
            for table, info in cls.model.items():
                setattr(cls, table, type(table, Table, {col.name: default_type(col.typ) for col in info.columns}))

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
    def __init__(self, mgr):
        self.__manager__ = mgr

    def insert(self, obj):
        """Insert an object into the db, object must support to_dict()."""
        vals = obj.to_dict()
        self.__manager__.db.insert(obj.__table_name__, vals)

    def update(self, obj):
        """Update the db from an object, object must support to_dict()."""
        vals = obj.to_dict()
        self.__manager__.db.update(obj.__table_name__, vals)

    def __select(self, cls, where):
        for row in self.__manager__.db.select(cls.__table_name__, None, where):
            yield cls.from_dict(row.__dict__)

    def select(self, cls, where={}, **kws):
        """Read objects of specified class."""
        kws.update(where)
        return RowIter(self.__select(cls, kws))

    def count(self, cls, where={}, **kws):
        """Return count of objs matchig where clause."""
        kws.update(where)
        return self.__manager__.db.count(cls.__table_name__, kws)

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
