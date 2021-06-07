import weakref
from contextlib import suppress
from typing import Set, Dict, Iterable, TYPE_CHECKING, TypeVar

from .errors import OmenNoPkError, OmenDuplicateObjectError
import logging as log

from .selectable import Selectable

if TYPE_CHECKING:
    from notanorm import DbBase
    from .omen import Omen
    from .object import ObjBase


T = TypeVar("T", bound="ObjBase")
U = TypeVar("U", bound="ObjBase")

# noinspection PyDefaultArgument,PyProtectedMember
class Table(Selectable[T]):
    # pylint: disable=dangerous-default-value, protected-access

    table_name: str
    field_names: Set[str]
    allow_auto: bool = False

    def __init_subclass__(cls, **_kws):
        if hasattr(cls, "row_type"):
            cls.row_type._table_type = cls

    def __init__(self, mgr: "Omen"):
        self.manager = mgr
        # noinspection PyTypeChecker
        self._cache: Dict[dict, "ObjBase"] = weakref.WeakValueDictionary()
        mgr.set_table(self)

    @property
    def db(self) -> "DbBase":
        return self.manager.db

    # noinspection PyCallingNonCallable
    def new(self, *a, **kw) -> T:
        """Convenience function to create a new row and add it to the db."""
        obj = self.row_type(*a, **kw)
        return self.add(obj)

    def add(self, obj: U) -> U:
        """Insert an object into the db"""
        obj._bind(table=self)
        obj._commit()
        return obj

    def remove(self, obj: "ObjBase" = None, **kws):
        """Remove an object from the db."""
        if obj is None:
            if not kws:
                log.debug("not removing obj, because it is None")
                return
            obj = self.select_one(**kws)
        if not obj or not obj._meta or not obj._meta.table:
            log.debug("not removing object that isn't in the db")
            return
        assert obj._meta.table is self
        obj._remove()

    def _remove(self, obj: "ObjBase"):
        """Remove an object from the db, without cascading."""
        self._cache.pop(obj._to_pk_tuple(), None)
        vals = obj._to_pk()
        self.db.delete(self.table_name, **vals)

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
            obj = self.row_type._from_db_not_new(row._asdict())
            cached: "ObjBase" = self._cache.get(obj._to_pk_tuple())
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

    def select(self, _where={}, **kws) -> Iterable[T]:
        """Read objects of specified class."""
        kws.update(_where)
        yield from self.__select(kws)

    def count(self, _where={}, **kws) -> int:
        """Return count of objs matchig where clause."""
        kws.update(_where)
        return self.db.count(self.table_name, kws)


# noinspection PyDefaultArgument,PyProtectedMember
class ObjCache(Selectable[T]):
    # pylint: disable=dangerous-default-value, protected-access

    def __init__(self, table: Table[T]):
        self.table = table
        self.table._cache = {}  # change the weak dict to a permanent dict

    def __getattr__(self, item):
        return getattr(self.table, item)

    def select(self, _where={}, **kws) -> Iterable[T]:
        kws.update(_where)
        for v in self.table._cache.values():
            if v._matches(kws):
                yield v

    def reload(self):
        return sum(1 for _ in self.table.select())

    def __iter__(self):
        return self.select()
