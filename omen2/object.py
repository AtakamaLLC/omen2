# pylint: disable=protected-access

import logging
from threading import RLock
from typing import Type, TYPE_CHECKING, Optional, Set

from dataclasses import dataclass

from omen2.errors import OmenError
from omen2.relation import Relation

if TYPE_CHECKING:
    from omen2.omen import Table, Omen

log = logging.getLogger(__name__)


@dataclass
class ObjMeta:
    lock = RLock()
    locked = False
    table: Optional["Table"] = None


# noinspection PyCallingNonCallable,PyProtectedMember
class ObjBase:
    # objects have 2 non-private attributes that can be overridden
    _pk: Set[str] = ()  # list of field names in the db used as the primary key
    _table_type: Type["Table"]  # class derived from Table

    # objects should only have 1 variable in __dict__
    _meta: ObjMeta = None

    def __eq__(self, obj):
        return obj._to_pk() == self._to_pk()

    def __hash__(self):
        return hash(self._to_pk_tuple())

    def _to_pk_tuple(self):
        return tuple(sorted(self._to_pk().items()))

    def __init__(self, **kws):
        """Override this to control initialization, generally calling it *after* you do your own init."""
        self._meta = ObjMeta()
        self._meta.lock = RLock()

        # you normally set these in the base class
        assert self._pk, "All classes must have a _pk"
        assert self._table_type, "All classes must have a table_type"

        self._check_kws(kws)

    @classmethod
    def _from_db(cls, dct):
        """Override this if you want to change how deserialization works."""
        return cls(**dct)

    def _check_kws(self, dct):
        for k in dct:
            if k not in self.__dict__:
                raise AttributeError(
                    "%s not a valid column in %s" % (k, self.__class__.__name__)
                )

    def _matches(self, dct):
        for k, v in dct.items():
            if getattr(self, k) != v:
                return False
        return True

    def _update(self, dct):
        self.__dict__.update(dct)

    def _bind(self, table: "Table" = None, manager: "Omen" = None):
        if table is None:
            table = getattr(manager, self._table_type.table_name)
        self._meta.table = table

    def _to_dict(self, keys=None):
        """Get dict of serialized data from self."""
        ret = {}
        for k, v in self.__dict__.items():
            if k[0] == "_":
                continue
            if k not in self._table_type.field_names:
                continue
            if keys and k not in keys:
                continue
            if hasattr(v, "to_db"):
                # pylint: disable=no-member
                v = v.to_db()
            if v is not None:
                ret[k] = v
        return ret

    def _to_pk(self):
        """Get dict of serialized data from self, but pk elements only."""
        return self._to_dict(self._pk)

    def _need_id(self):
        need_id_field = None
        for field in self._pk:
            if getattr(self, field, None) is None:
                assert not need_id_field
                need_id_field = field
        return need_id_field

    def __setattr__(self, k, v):
        if k[0] == "_":
            super().__setattr__(k, v)
            return

        if self._meta and self._meta.table and not self._meta.locked:
            raise OmenError("use with: protocol for bound objects")

        if self._meta and not hasattr(self, k):
            raise AttributeError("Attribute %s not defined" % k)
        super().__setattr__(k, v)

    def _commit(self):
        if self._meta.table:
            self._save()
        for val in self.__dict__.values():
            if isinstance(val, Relation):
                val.commit(self._meta.table.manager)

    def _save(self):
        need_id_field = self._need_id()
        table = self._meta.table
        if need_id_field:
            table.insert(self, need_id_field)
        else:
            table.update(self)

    def _rollback(self):
        pk = self._to_pk()
        self._update(self._meta.table.select_one(**pk))

    def __enter__(self):
        if self._meta and not self._meta.table:
            self._meta.lock.acquire()
            self._meta.locked = True
        return self

    def __exit__(self, typ, val, ex):
        # unbound objects aren't locked, and don't need the with: protocol
        if not self._meta or not self._meta.table:
            return

        if typ:
            self._rollback()
        else:
            self._commit()

        self._meta.locked = False
        self._meta.lock.release()
