# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Omen2: Table class and supporting types"""
import contextlib
import threading
import weakref
from contextlib import suppress
from enum import Enum
from typing import Set, Dict, Iterable, TYPE_CHECKING, TypeVar, Tuple, Generator

from .errors import OmenNoPkError, OmenRollbackError, IntegrityError
import logging as log

from .selectable import Selectable
from .object import ObjBase

if TYPE_CHECKING:
    from notanorm import DbBase
    from .omen import Omen

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
    UPSERT = 4


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
        self.locked_objs: Set["ObjBase"] = set()
        self.lock = threading.RLock()

        mgr.set_table(self)

    @property
    def db(self) -> "DbBase":
        """Get bound db."""
        return self.manager.db

    # noinspection PyCallingNonCallable
    def new(self, *a, **kw) -> T:
        """Convenience function to create a new row and add it to the db.

        Equivalent to: table.add(Object(*a, **kw))

        """
        obj = self.row_type(*a, **kw)
        return self.add(obj)

    def upsert(self, *a, _insert_only=None, **kw) -> T:
        """Update row in db if present, otherwise, insert row.

        table.upsert(Object(...))

        or

        table.upsert(key2=val1, key2=val2)

        Arg :_insert_only: is a dict of values that are used when inserting and constructing
        the insertion object, but are ignored when updatin.

        Note: If using the keyword-version of this function, all values that
              are not indicated by the keywords will retain the values of the existing row.
        """
        if a and isinstance(a[0], ObjBase):
            obj = a[0]
            assert len(a) == 1, "only one object allowed"
            assert not kw, "cannot mix kw and obj upsert"
        else:
            up_fds = kw.keys()
            if _insert_only:
                up_fds = set(up_fds) - set(_insert_only.keys())
                kw.update(_insert_only)
            obj = self.row_type(*a, **kw)
            obj._set_up_fds(up_fds)
        return self._add(obj, upsert=True)

    def add(self, obj: U) -> U:
        """Insert an object into the db"""
        return self._add(obj, upsert=False)

    def _add(self, obj: U, upsert: bool) -> U:
        if self._in_tx():
            tid = threading.get_ident()
            if obj in self._tx_objs[tid]:
                for sub in self._tx_objs[tid]:
                    if obj == sub:
                        if id(obj) != id(sub):
                            raise IntegrityError
            if upsert:
                op = TxStatus.UPSERT
            else:
                op = TxStatus.ADD
            self._tx_objs[tid][obj] = op
            return obj
        else:
            return self._notx_add(obj, upsert)

    def _notx_add(self, obj: U, upsert: bool = False) -> U:
        obj._bind(table=self)
        obj._commit(upsert=upsert)
        if upsert:
            pk = obj._to_pk_tuple()
            obj = self._cache.get(pk, obj)
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

    def remove_all(self, **kws):
        """Remove all matching objects from the db."""
        if self._in_tx():
            for obj in self.select(**kws):
                self._db_remove(obj)
        else:
            self.db.delete(self.table_name, **kws)
            pop = []
            for obj in self._cache.values():
                if obj._matches(kws):
                    pop.append(obj._to_pk_tuple())
            for ent in pop:
                self._cache.pop(ent)

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
        """Update object db + cache"""
        # called from table.py when a bound object is modified
        vals = obj._to_db(keys)
        self.db.update(self.table_name, obj._saved_pk, **vals)
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

    def db_upsert(self, obj: T, id_field, up_fds):
        """Upsert the db + cache from object."""
        vals = obj._to_db()
        insonly = {}

        if up_fds:
            newv = {}
            for k, v in vals.items():
                if k in up_fds:
                    newv[k] = v
                else:
                    insonly[k] = v
            vals = newv

        ret = self.db.upsert(self.table_name, _insert_only=insonly, **vals)

        # force id's in there
        if id_field and hasattr(ret, "lastrowid"):
            obj.__dict__[id_field] = ret.lastrowid

        pk = obj._to_pk_tuple()
        alr = self._cache.get(pk)
        if alr:
            alr._update_from_object(obj)
            return
        self._add_cache(obj)

    def db_select(self, where):
        """Call select on the underlying db, given a where dict of keys/values."""
        return self.db.select(self.table_name, None, where)

    def db_select_gen(self, where, order_by=None):
        """Call select_gen on the underlying db, given a where dict of keys/values."""
        yield from self.db.select_gen(self.table_name, None, where, order_by=order_by)

    def __select_intx(self, where) -> Generator[T, None, None]:
        if self._in_tx():
            tid = threading.get_ident()
            for obj, status in self._tx_objs.get(tid, {}).items():
                if status not in (TxStatus.ADD, TxStatus.UPSERT):
                    continue
                if obj._matches(where):
                    yield obj

    def __select(self, where, _order_by=None) -> Generator[T, None, None]:
        db_pks = set()
        db_where = {k: v for k, v in where.items() if k in self.field_names}
        attr_where = {k: v for k, v in where.items() if k not in self.field_names}
        for row in self.db_select_gen(db_where, order_by=_order_by):
            obj = self.row_type._from_db_not_new(row._asdict())
            pk = obj._to_pk_tuple()
            db_pks.add(pk)
            if self._in_tx():
                tid = threading.get_ident()
                status = self._tx_objs[tid].get(obj, None)
                if status and status != TxStatus.UPDATE:
                    continue
            with self.lock:
                cached: "ObjBase" = self._cache.get(pk)
                if cached:
                    if not cached._is_locked() and obj._to_db() != cached._to_db():
                        log.debug("updating %r from db", obj)
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

    def select(self, _where={}, _order_by=None, **kws) -> Generator[T, None, None]:
        """Read objects of specified class.

        Specify _order_by="field" or ["field1 desc", "field2"] to sort the results.
        """
        kws.update(_where)
        yield from self.__select(kws, _order_by=_order_by)

    def count(self, _where={}, **kws) -> int:
        """Return count of objs matching where clause."""
        kws.update(_where)
        return self.db.count(self.table_name, kws)

    def _wait_for_locked_objects(self):
        try:
            locked_obj = self.locked_objs.pop()
            while locked_obj:
                with locked_obj._lock:
                    pass
                locked_obj = self.locked_objs.pop()
        except KeyError:
            pass

    @contextlib.contextmanager
    def _unsafe_transaction(self):
        with self.lock:
            self._wait_for_locked_objects()
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
                            self._notx_add(obj, upsert=False)
                        elif status == TxStatus.UPSERT:
                            self._notx_add(obj, upsert=True)
                        elif status == TxStatus.REMOVE:
                            self._notx_remove(obj)
                        needs_rollback.add((obj, status))
            except Exception as e:
                for obj, status in self._tx_objs[tid].items():
                    if obj not in needs_rollback:
                        obj.__exit__(type(e), e, None)
                # update cache from objs that appeared committed
                for obj, status in needs_rollback:
                    if status in (TxStatus.ADD, TxStatus.UPSERT):
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

    def select(self, _where={}, **kws) -> Generator[T, None, None]:
        """Read objects from the cache."""
        kws.update(_where)
        for v in self.table._cache.values():
            if v._matches(kws):
                yield v

    def reload(self):
        """Reload the objects in the cache from the db."""
        with self.table.lock:
            return sum(1 for _ in self.table.select())
