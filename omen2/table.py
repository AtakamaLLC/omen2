import weakref
from typing import Set

from .object import ObjBase
from .errors import OmenMoreThanOneError


# noinspection PyDefaultArgument,PyProtectedMember
class Table:
    # pylint: disable=dangerous-default-value, protected-access

    table_name: str
    row_type: ObjBase
    field_names: Set[str]

    def __init_subclass__(cls, **_kws):
        cls.row_type._table_type = cls

    def __init__(self, mgr):
        self.manager = mgr
        self.__cache = weakref.WeakValueDictionary()

    @property
    def db(self):
        return self.manager.db

    def add(self, obj):
        """Insert an object into the db"""
        self.__cache[obj._to_pk_tuple()] = obj
        obj._bind(table=self)
        obj._commit()
        return obj

    def remove(self, obj: ObjBase):
        """Remove an object from the db."""
        self.__cache.pop(obj._to_pk_tuple(), None)
        vals = obj._to_pk()
        self.db.delete(**vals)

    def update(self, obj: "ObjBase"):
        """Add object to db + cache"""
        self.__cache[obj._to_pk_tuple()] = obj
        vals = obj._to_dict()
        self.db.upsert(self.table_name, **vals)

    def insert(self, obj: "ObjBase", id_field):
        """Update the db + cache from object."""
        self.__cache[obj._to_pk_tuple()] = obj
        vals = obj._to_dict()
        ret = self.db.insert(self.table_name, **vals)
        with obj:
            setattr(obj, id_field, ret.lastrowid)

    def db_select(self, where):
        return self.db.select(self.table_name, None, where)

    def __select(self, where):
        for row in self.db_select(where):
            obj = self.row_type._from_db(row.__dict__)
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
