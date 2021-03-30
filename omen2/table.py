import weakref
from contextlib import suppress
from typing import Set, Dict, Generic, Iterable, TypeVar, TYPE_CHECKING

from .object import ObjBase
from .errors import OmenMoreThanOneError, OmenNoPkError, OmenDuplicateObjectError
import logging as log

if TYPE_CHECKING:
    from omen2.omen import Omen

T = TypeVar("T", bound=ObjBase)


# noinspection PyDefaultArgument,PyProtectedMember
class Table(Generic[T]):
    # pylint: disable=dangerous-default-value, protected-access

    table_name: str
    row_type: ObjBase
    field_names: Set[str]

    def __init_subclass__(cls, **_kws):
        if hasattr(cls, "row_type"):
            cls.row_type._table_type = cls

    def __init__(self, mgr: "Omen"):
        self.manager = mgr
        self._cache: Dict[dict, ObjBase] = weakref.WeakValueDictionary()

    @property
    def db(self):
        return self.manager.db

    def new(self, *a, **kw):
        obj = self.row_type(*a, **kw)
        return self.add(obj)

    def add(self, obj):
        """Insert an object into the db"""
        obj._bind(table=self)
        obj._commit()
        return obj

    def remove(self, obj: T):
        """Remove an object from the db."""
        self._cache.pop(obj._to_pk_tuple(), None)
        vals = obj._to_pk()
        self.db.delete(**vals)

    def update(self, obj: T):
        """Add object to db + cache"""
        self._add_cache(obj)
        vals = obj._to_db()
        if obj._meta.pk:
            self.db.upsert(self.table_name, obj._meta.pk, **vals)
        else:
            self.db.upsert(self.table_name, **vals)

    def _add_cache(self, obj: T):
        with suppress(OmenNoPkError):
            pk = obj._to_pk_tuple()
        alr = self._cache.get(pk)
        if alr is not None and alr is not obj:
            raise OmenDuplicateObjectError
        self._cache[pk] = obj

    def insert(self, obj: T, id_field):
        """Update the db + cache from object."""
        vals = obj._to_db()
        ret = self.db.insert(self.table_name, **vals)
        # force id's in there
        if id_field:
            obj.__dict__[id_field] = ret.lastrowid
        self._add_cache(obj)

    def db_select(self, where):
        return self.db.select(self.table_name, None, where)

    def __select(self, where) -> Iterable[T]:
        for row in self.db_select(where):
            obj = self.row_type._from_db_not_new(row.__dict__)
            cached: ObjBase = self._cache.get(obj._to_pk_tuple())
            if cached:
                update = obj._to_db()
                already = cached._to_db()
                if update != already:
                    log.debug("updating %s from db", repr(obj))
                    with obj:
                        obj._update(update)
                obj = cached
            else:
                obj._bind(table=self)
                self._add_cache(obj)
            yield obj

    def select(self, where={}, **kws):
        """Read objects of specified class."""
        kws.update(where)
        yield from self.__select(kws)

    def get(self, *args, **kws):
        """Shortcut method, you can access object by pk/positional args."""
        for i, v in enumerate(args):
            kws[self.row_type._pk[i]] = v
        return self.select_one(**kws)

    def __iter__(self):
        return self.select()

    def count(self, where={}, **kws):
        """Return count of objs matchig where clause."""
        kws.update(where)
        return self.db.count(self.table_name, kws)

    def select_one(self, where={}, **kws):
        """Return one row, None, or raises an OmenMoreThanOneError."""
        itr = self.select(where, **kws)
        return self._return_one(itr)

    @staticmethod
    def _return_one(itr):
        try:
            one = next(itr)
        except StopIteration:
            return None

        try:
            next(itr)
            raise OmenMoreThanOneError
        except StopIteration:
            return one


# noinspection PyDefaultArgument,PyProtectedMember
class ObjCache:
    # pylint: disable=dangerous-default-value, protected-access

    def __init__(self, table: Table):
        self.table = table
        self.table._cache = {}  # change the weak dict to a permanent dict

    def __getattr__(self, item):
        return getattr(self.table, item)

    def select(self, where={}, **kws):
        kws.update(where)
        for v in self.table._cache.values():
            if v._matches(kws):
                yield v

    def select_one(self, where={}, **kws):
        itr = self.select(where, **kws)
        return self._return_one(itr)

    def reload(self):
        return sum(1 for _ in self.table.select())

    def __iter__(self):
        return self.select()
