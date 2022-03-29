# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Omen2: Table class and supporting types"""
import contextlib
import threading
import weakref
from contextlib import suppress
from enum import Enum
from typing import Set, Dict, Iterable, TYPE_CHECKING, TypeVar, Tuple

from .errors import OmenNoPkError, OmenRollbackError, IntegrityError
import logging as log

from .selectable import Selectable

if TYPE_CHECKING:
    from notanorm import DbBase
    from .omen import Omen
    from .object import ObjBase

T = TypeVar("T", bound="ObjBase")
U = TypeVar("U", bound="ObjBase")


class TxStatus(Enum):
    """Status of objects in per-thread transaction cache.
    UPDATE: object was edited
    ADD: object was added
    REMOVE: object was removed
    """

    UPDATE = 1
    ADD = 2
    REMOVE = 3


# noinspection PyDefaultArgument,PyProtectedMember
class Table(Selectable[T]):
    """Omen2: Table base class from which tables are derived."""

    # pylint: disable=dangerous-default-value, protected-access

    table_name: str
    field_names: Set[str]
    allow_auto: bool = None

    def __init_subclass__(cls, *_a, **_kws):
        if hasattr(cls, "row_type"):
            cls.row_type._table_type = cls

    def __init__(self, mgr: "Omen"):
        """Bind table to omen manager."""
        self.manager = mgr
        # noinspection PyTypeChecker
        self._cache: Dict[dict, "ObjBase"] = weakref.WeakValueDictionary()

        self._tx_objs: Dict[int, Dict["ObjBase", TxStatus]] = {}

        mgr.set_table(self)

    @property
    def db(self) -> "DbBase":
        """Get bound db."""
        return self.manager.db

    # noinspection PyCallingNonCallable
    def new(self, *a, **kw) -> T:
        """Convenience function to create a new row and add it to the db."""
        obj = self.row_type(*a, **kw)
        return self.add(obj)

    def add(self, obj: U) -> U:
        """Insert an object into the db"""
        if self._in_tx():
            tid = threading.get_ident()
            if obj in self._tx_objs[tid]:
                for sub in self._tx_objs[tid]:
                    if obj == sub:
                        if id(obj) != id(sub):
                            raise IntegrityError
            self._tx_objs[tid][obj] = TxStatus.ADD
            return obj
        else:
            return self._notx_add(obj)

    def _notx_add(self, obj: U) -> U:
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
        if not obj or not obj._is_bound:
            log.debug("not removing object that isn't in the db")
            return
        assert obj._table is self
        obj._remove()

    def _db_remove(self, obj: "ObjBase"):
        """Remove an object from the db, without cascading."""
        if self._in_tx():
            tid = threading.get_ident()
            self._tx_objs[tid][obj] = TxStatus.REMOVE
            return
        self._notx_remove(obj)

    def _notx_remove(self, obj: "ObjBase"):
        self._cache.pop(obj._to_pk_tuple(), None)
        vals = obj._to_pk()
        self.db.delete(self.table_name, **vals)

    def update(self, obj: T, keys: Iterable[str]):
        """Add object to db + cache"""
        vals = obj._to_db(keys)
        if obj._saved_pk:
            self.db.upsert(self.table_name, obj._saved_pk, **vals)
        else:
            self.db.upsert(self.table_name, **vals)
        self._add_cache(obj)

    def _add_cache(self, obj: T):
        with suppress(OmenNoPkError):
            pk = obj._to_pk_tuple()
        alr = self._cache.get(pk)
        self._cache[pk] = obj
        if alr is not None and alr is not obj:
            # update old refs as best we can
            alr._update_from_object(obj)

    def db_insert(self, obj: T, id_field):
        """Update the db + cache from object."""
        vals = obj._to_db()
        ret = self.db.insert(self.table_name, **vals)
        # force id's in there
        if id_field:
            obj.__dict__[id_field] = ret.lastrowid
        self._add_cache(obj)

    def db_select(self, where):
        """Call select on the underlying db, given a where dict of keys/values."""
        return self.db.select(self.table_name, None, where)

    def __select_intx(self, where) -> Iterable[T]:
        if self._in_tx():
            tid = threading.get_ident()
            for obj, status in self._tx_objs.get(tid, {}).items():
                if status != TxStatus.ADD:
                    continue
                if obj._matches(where):
                    yield obj

    def __select(self, where) -> Iterable[T]:
        db_pks = set()
        db_where = {k: v for k, v in where.items() if k in self.field_names}
        attr_where = {k: v for k, v in where.items() if k not in self.field_names}
        for row in self.db_select(db_where):
            obj = self.row_type._from_db_not_new(row._asdict())
            pk = obj._to_pk_tuple()
            cached: "ObjBase" = self._cache.get(pk)
            db_pks.add(pk)
            if self._in_tx():
                tid = threading.get_ident()
                status = self._tx_objs[tid].get(obj, None)
                if status and status != TxStatus.UPDATE:
                    continue
            if cached:
                if obj._to_db() != cached._to_db():
                    log.debug("updating %s from db", repr(obj))
                    cached._update_from_object(obj)
                obj = cached
            else:
                obj._bind(table=self)
                self._add_cache(obj)
            if obj._matches(attr_where):
                yield obj

        yield from self.__select_intx(where)

        self.__clean_cache(where, db_pks)

    def __clean_cache(self, where, db_pks):
        # remove cached items that are no longer in the db
        remove_from_cache = set()
        for k, v in self._cache.items():
            if v._matches(where) and k not in db_pks:
                remove_from_cache.add(k)
        for pop_me in remove_from_cache:
            log.debug("removing %s from cache", pop_me)
            self._cache.pop(pop_me)

    def select(self, _where={}, **kws) -> Iterable[T]:
        """Read objects of specified class."""
        kws.update(_where)
        yield from self.__select(kws)

    def count(self, _where={}, **kws) -> int:
        """Return count of objs matching where clause."""
        kws.update(_where)
        return self.db.count(self.table_name, kws)

    @contextlib.contextmanager
    def _unsafe_transaction(self):
        tid = threading.get_ident()
        self._tx_objs[tid] = {}
        needs_rollback: Set[Tuple["ObjBase", TxStatus]] = set()
        try:
            with self.db.transaction():
                yield self
                for obj, status in self._tx_objs[tid].items():
                    if status == TxStatus.UPDATE:
                        obj.__exit__(None, None, None)
                    elif status == TxStatus.ADD:
                        self._notx_add(obj)
                    elif status == TxStatus.REMOVE:
                        self._notx_remove(obj)
                    needs_rollback.add((obj, status))
        except Exception as e:
            for obj, status in self._tx_objs[tid].items():
                if obj not in needs_rollback:
                    obj.__exit__(type(e), e, None)
            # update cache from objs that appeared committed
            for obj, status in needs_rollback:
                if status == TxStatus.ADD:
                    self._cache.pop(obj._to_pk_tuple(), None)
                else:
                    self.select(**obj._to_pk())
            # propagate error
            raise
        finally:
            del self._tx_objs[tid]

    @contextlib.contextmanager
    def transaction(self):
        """Use in a with block to enter a transaction on this table only."""
        try:
            with self._unsafe_transaction():
                yield
        except OmenRollbackError:
            pass

    def _in_tx(self):
        return threading.get_ident() in self._tx_objs

    def _add_object_to_tx(self, obj: "ObjBase"):
        assert self._in_tx()
        objs = self._tx_objs[threading.get_ident()]
        if obj not in objs:
            obj.__enter__()
            objs[obj] = TxStatus.UPDATE


# noinspection PyDefaultArgument,PyProtectedMember
class ObjCache(Selectable[T]):
    """Omen2 object cache: same interface as table, but all objects are preloaded."""

    # pylint: disable=dangerous-default-value, protected-access

    def __init__(self, table: Table[T]):
        self.table = table
        self.table._cache = {}  # change the weak dict to a permanent dict

    def __getattr__(self, item):
        """Pass though to table on everything but select."""
        return getattr(self.table, item)

    def select(self, _where={}, **kws) -> Iterable[T]:
        """Read objects from the cache."""
        kws.update(_where)
        for v in self.table._cache.values():
            if v._matches(kws):
                yield v

    def reload(self):
        """Reload the objects in the cache from the db."""
        return sum(1 for _ in self.table.select())
