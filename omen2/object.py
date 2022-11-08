# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Omen2 object and associated classes."""

# pylint: disable=protected-access

import logging
import threading
from threading import RLock
from typing import Type, TYPE_CHECKING, Optional, Tuple, Iterable, Dict, Any, Union

from dataclasses import dataclass
from contextlib import contextmanager

from .errors import OmenUseWithError, OmenNoPkError, OmenRollbackError, OmenLockingError
from .relation import Relation

if TYPE_CHECKING:
    from omen2.omen import Omen
    from omen2 import Table

log = logging.getLogger(__name__)


@dataclass
class ObjMeta:
    """Object private metadata containing the bound table, a lock, and other flags."""

    def __init__(self):
        self.lock = RLock()
        self.locked = False
        self.new = True
        self.table: Optional["Table"] = None
        self.pk = None
        self.lock_id = 0
        self.suppress_set_changes = False
        self.suppress_get_changes = False
        self.changes: Dict[str, Any] = None
        self.in_sync = False
        self.up_fds = None


VERY_LARGE_LOCK_TIMEOUT = 120

# noinspection PyCallingNonCallable,PyProtectedMember
class ObjBase:
    """Object base class, from which all objects are derived."""

    # objects have 2 non-private attributes that can be overridden
    _cascade = True
    _type_check = None  # whether annotated types are checked in python
    _pk: Tuple[str, ...] = ()  # list of field names in the db used as the primary key
    _table_type: Type["Table"]  # class derived from Table
    _sync_on_getattr = False  # maybe don't use this feature, it's an "ipc hack"

    # objects should only have 1 variable in __dict__
    __meta: ObjMeta = None

    # getattr optimization, because python is slow
    __need_attr: bool = False

    def __eq__(self, obj):
        return obj._to_pk() == self._to_pk()

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self._to_pk(unsafe=True)) + ")"

    def __str__(self):
        return str(self._to_db())

    def __hash__(self):
        try:
            return hash(self._to_pk_tuple())
        except OmenNoPkError:
            return id(self)

    def _to_pk_tuple(self):
        return tuple(sorted(self._to_pk().items()))

    def __lt__(self, other: "ObjBase"):
        return self._to_pk_tuple() < other._to_pk_tuple()

    def __init_subclass__(cls, **_kws):
        # you must set these in the base class
        assert cls._pk, "All classes must have a _pk"

    def __init__(self, **kws):
        """Override this to control initialization, generally calling it *after* you do your own init."""
        # even though this is set at the top of __init__, the __meta variable
        # may not be set in a subclass before super().__init__ is called
        # that means all attribute refs that use __meta, have to check 'if __meta' first
        self.__meta = ObjMeta()
        self._check_kws(kws)
        self.__meta.new = True

    def _set_up_fds(self, up_fds):
        self.__meta.up_fds = up_fds

    def _save_pk(self):
        self.__meta.pk = self._to_pk()

    @property
    def _changes(self):
        return self.__meta.changes

    @property
    def _saved_pk(self):
        return self.__meta.pk

    @classmethod
    def _from_db(cls, dct):
        """Override this if you want to change how deserialization works."""
        return cls(**dct)

    def _link_custom_types(self):
        """Any values derived from CustomType will track-changes through to the parent object."""
        for k, v in self.__dict__.items():
            if isinstance(v, CustomType):
                # pylint: disable=attribute-defined-outside-init
                v._parent = self
                v._field = k

    @classmethod
    def _from_db_not_new(cls, dct):
        """Override this if you want to change how deserialization works."""
        ret = cls._from_db(dct)
        ret._link_custom_types()
        ret.__meta.new = False
        # all db-bound objects have a saved pk
        ret._save_pk()
        return ret

    def _check_kws(self, dct):
        for k in dct:
            if k not in self.__dict__:
                raise AttributeError(
                    "%s not a known attribute of %s" % (k, self.__class__.__name__)
                )

    def _matches(self, dct):
        for k, v in dct.items():
            if getattr(self, k) != v:
                return False
        return True

    def _update_from_object(self, obj):
        update = {
            k: v
            for k, v in obj.__dict__.items()
            if (
                (not obj.__meta.up_fds or k in obj.__meta.up_fds)
                and not isinstance(v, Relation)
                and not k.startswith("_ObjBase__")
            )
        }
        self._atomic_apply(self, update)

    def _bind(self, table: "Table" = None, manager: "Omen" = None):
        if table is None:
            table = manager[self._table_type]
        self._table = table

    @property
    def _is_bound(self) -> bool:
        # important to cast this for better errors
        return bool(self.__meta and self.__meta.table is not None)

    @property
    def _is_new(self) -> bool:
        return self.__meta and self.__meta.new

    @property
    def _table(self):
        return self.__meta.table

    @property
    def _lock(self):
        return self.__meta.lock

    @_table.setter
    def _table(self, val):
        self.__meta.table = val

    @property
    def _manager(self):
        return self.__meta.table.manager

    @contextmanager
    def _suppress_get_changes(self):
        self.__meta.suppress_get_changes = True
        yield self
        self.__meta.suppress_get_changes = False

    @contextmanager
    def _suppress_set_changes(self):
        self.__meta.suppress_set_changes = True
        yield self
        self.__meta.suppress_set_changes = False

    def _to_db(self, keys: Iterable[str] = None):
        """Get dict of serialized data from self."""
        ret = {}
        keys = keys or self._table_type.field_names
        with self._suppress_get_changes():
            for k in keys:
                v = getattr(self, k)
                if hasattr(v, "_to_db"):
                    # pylint: disable=no-member
                    v = v._to_db()

                ret[k] = v
            return ret

    def _to_pk(self, unsafe=False):
        """Get dict of serialized data from self, but pk elements only.

        You will have to override this if you're overriding _to_db, and
        any of the transformed values are part of your primary key.

        Guaranteed compatible override would be:

        def _to_pk(self):
            dct = self._to_db()
            return {k: dct[v] for k in self._pk}

        The only reason this isn't used by default is efficiency.
        """
        ret = {}
        for k in self._pk:
            v = getattr(self, k)
            if v is None and not unsafe:
                raise OmenNoPkError("invalid primary key")
            ret[k] = v
        return ret

    def _need_id(self):
        need_id_field = None
        for field in self._pk:
            if getattr(self, field, None) is None:
                if not self._table_type.allow_auto:
                    raise OmenNoPkError(
                        "will not create %s without primary key"
                        % self.__class__.__name__
                    )
                assert not need_id_field
                need_id_field = field
        return need_id_field

    def __getattribute__(self, k):
        if k[0] == "_":
            return object.__getattribute__(self, k)

        if self.__need_attr:
            # in the middle of making changes, if this is the same thread, make them visible
            if (
                self.__meta
                and self.__meta.locked
                and self.__meta.lock_id == threading.get_ident()
                and not self.__meta.suppress_get_changes
            ):
                return self.__meta.changes.get(k, object.__getattribute__(self, k))

        if self._sync_on_getattr:
            # this should probably never be used, deprecate it
            self._syncattr(k)

        return object.__getattribute__(self, k)

    def _syncattr(self, k):
        if (
            self._sync_on_getattr
            and self._is_bound
            and not self.__meta.locked
            and not self.__meta.in_sync
        ):
            self.__meta.in_sync = True
            try:
                res = self._table.db_select(self._to_pk())[0]
                if k in res:
                    v = res[k]
                    object.__setattr__(self, k, v)
            finally:
                self.__meta.in_sync = False

    @classmethod
    def __get_type(cls, k) -> Type:  # pylint: disable=unused-private-member
        if k not in cls.__annotations__:
            typ = None
            for c in cls.mro():
                ann = getattr(c, "__annotations__", {})
                if k in ann:
                    typ = ann.get(k, None)
            # cache result
            cls.__annotations__[k] = typ
            return typ
        return cls.__annotations__[k]

    @staticmethod
    def __accept_instance(v, typ):  # pylint: disable=unused-private-member
        if typ is Any:
            return True
        if type(v) is int and issubclass(typ, float):
            return True
        try:
            return isinstance(v, typ)
        except TypeError:
            # not checking types i don't know how to handle
            return True

    @classmethod
    def __assert_instance(cls, k, v, typ):
        if getattr(typ, "__origin__", None) is Union:
            for sub in typ.__args__:
                if cls.__accept_instance(v, sub):
                    return
        elif cls.__accept_instance(v, typ):
            return
        raise TypeError("%s is type %s" % (k, typ))

    def _checkattr(self, k, v):
        """Override this if you want to change how annotation checking is done.

        This only does very basic assertions, and will not check complex types.
        """
        if not hasattr(self, k):
            raise AttributeError("Attribute %s not defined" % k)
        self._checktype(k, v)

    def _checktype(self, k, v):
        """Check if type of value is allowed."""
        if self._type_check:
            typ = self.__get_type(k)
            if typ:
                self.__assert_instance(k, v, typ)

    def __setattr__(self, k, v):
        if k[0] == "_":
            object.__setattr__(self, k, v)
            return

        if self.__meta:
            self._checkattr(k, v)
        else:
            self._checktype(k, v)

        if self.__meta and not self.__meta.suppress_set_changes:
            if (
                self.__meta.table is not None
                and self.__meta.table._in_tx()
                and not self.__meta.locked
            ):
                self.__meta.table._add_object_to_tx(self)

            if self.__meta.table is not None and not self.__meta.locked:
                raise OmenUseWithError("use with: protocol for bound objects")
            if (
                self.__meta.table is not None
                and self.__meta.lock_id != threading.get_ident()
            ):
                raise OmenUseWithError("use with: protocol for bound objects")

        if self._is_bound and not self.__meta.suppress_set_changes:
            self.__meta.changes[k] = v
            self.__need_attr = True
        else:
            object.__setattr__(self, k, v)

    def _get_related(self):
        related = {}
        for val in self.__dict__.values():
            if isinstance(val, Relation) and val.cascade:
                related[val] = list(val.select())
        return related

    def _collect_cascade(self):
        # cascading id changes to relations is expensive, and involves weird swaps
        # but it seems like this should be rare behavior
        # we save a list of all related objects here
        # and then later, we go throught and update all of them
        # because we don't know what the lambda-relationship is
        if self.__meta.pk:
            pk = self._to_pk()
            if pk != self.__meta.pk:
                for k, v in self.__meta.pk.items():
                    setattr(self, k, v)
                try:
                    return self._get_related()
                finally:
                    for k, v in pk.items():
                        setattr(self, k, v)
        return {}

    @staticmethod
    def _atomic_apply(obj, changes: Dict[str, Any]):
        """Atomically apply a dictionary of changes to a python object."""
        tmpobj = obj.__new__(type(obj))  # new obj, no __init__
        tmpobj.__dict__ = obj.__dict__.copy()  # copy all attrs to new obj
        tmpobj._force_apply(changes)
        obj.__dict__ = tmpobj.__dict__  # swap in new dict (atomic)

    def _force_apply(self, changes):
        # this happens when the underlying database changes
        # these attribute sets do not go into the changeset
        # but setters still get triggered normally
        # wait for everyone else to be done writing
        with self._lock:
            with self._suppress_set_changes():
                # sneak attrs in
                for k, v in changes.items():
                    setattr(self, k, v)

    def _commit(self, upsert=False):
        """Apply all pending changes to the object, and to the db."""
        changes = []

        # apply all changes to this object & triggers setters
        if self.__meta.changes:
            # apply changes to new obj, side effects of setters, etc
            changes = self.__meta.changes
            self._atomic_apply(self, changes)
            self.__meta.changes = {}
            self.__need_attr = False

        # collect any primary key-cascading updates
        cascade = self._collect_cascade() if self._cascade else {}

        # save changes to the db
        self._save(changes, upsert=upsert)

        # commit any changes in unbound relations to the db
        for val in self.__dict__.values():
            if isinstance(val, Relation):
                val.commit(self.__meta.table.manager)

        # apply any cascading primary-key changes to the db
        for rel, objs in cascade.items():
            for obj in objs:
                rel._link_obj(obj)

        # custom types can lose their linkage during this process
        self._link_custom_types()

    def _remove(self):
        """Remove myself from my table.

        Normally cascades to related tables, if Class._cascade is true.
        """
        cascade = self._get_related() if self._cascade else {}

        for rel, objs in cascade.items():
            for obj in objs:
                rel.remove(obj)

        if self.__meta.table is not None:
            table = self.__meta.table
            table._db_remove(self)

    def _save(self, keys: Iterable[str], upsert: bool):
        """Save myself to my table."""

        need_id_field = self._need_id()
        table = self.__meta.table
        if need_id_field or self.__meta.new:
            if upsert:
                table.db_upsert(self, need_id_field, self.__meta.up_fds)
            else:
                table.db_insert(self, need_id_field)
        elif keys:
            # update bound object
            table.update(self, keys)

        self.__meta.new = False
        self._save_pk()

    def _is_locked(self):
        return self.__meta.locked

    def __enter__(self):
        """Lock for write, and trigger thread-isolation."""
        if self.__meta and self.__meta.table is not None:
            with self._table.lock:
                if not self._lock.acquire(timeout=VERY_LARGE_LOCK_TIMEOUT):
                    log.critical("deadlock prevented", stack_info=True)
                    raise OmenLockingError
                if self.__meta.locked:
                    # nested with blocks could work, but they are an anti-pattern
                    self._lock.release()
                    raise OmenLockingError("nested with blocks not supported")
                self.__meta.locked = True
                self.__meta.changes = {}
                self.__need_attr = False
                self.__meta.lock_id = threading.get_ident()
                self._table.locked_objs.add(self)
            if self._sync_on_getattr:
                res = self._table.db_select(self._to_pk())[0]
                for k, v in res.items():
                    object.__setattr__(self, k, v)
        return self

    def __exit__(self, typ, ex, tb):
        """Finished with write, call commit or not, based on exception."""
        if not self.__meta or not self.__meta.locked:
            # unbound objects aren't locked, and don't need the with: protocol
            return False

        try:
            if not typ:
                # see self._atomic_update() for why this works
                saved = self.__dict__  # pylint: disable=access-member-before-definition
                try:
                    self._commit()
                except Exception:
                    self.__dict__ = saved
                    raise
            if typ is OmenRollbackError:
                return True
        finally:
            self.__meta.locked = False
            self.__meta.changes = None
            self.__need_attr = False
            self.__meta.lock_id = 0
            self._lock.release()
            self._table.locked_objs.discard(self)

        return False


class CustomType:
    """Derive from this type so that track-changes works with your custom object."""

    _parent = None
    _field = None

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        if self._parent and self._field and key[0] != "_":
            setattr(self._parent, self._field, self)
