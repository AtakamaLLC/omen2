# pylint: disable=protected-access

import logging
from threading import RLock
from typing import Type, TYPE_CHECKING, Optional, Tuple

from dataclasses import dataclass

from .errors import OmenError, OmenMoreThanOneError, OmenNoPkError
from .relation import Relation

if TYPE_CHECKING:
    from omen2.omen import Omen
    from omen2 import Table

log = logging.getLogger(__name__)


@dataclass
class ObjMeta:
    lock = RLock()
    locked = False
    new = True
    table: Optional["Table"] = None
    pk = None
    restore = None


# noinspection PyCallingNonCallable,PyProtectedMember
class ObjBase:
    # objects have 2 non-private attributes that can be overridden
    _cascade = True
    _pk: Tuple[str, ...] = ()  # list of field names in the db used as the primary key
    _table_type: Type["Table"]  # class derived from Table

    # objects should only have 1 variable in __dict__
    _meta: ObjMeta = None

    def __eq__(self, obj):
        return obj._to_pk() == self._to_pk()

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self._to_pk()) + ")"

    def __str__(self):
        return str(self._to_db())

    def __hash__(self):
        return hash(self._to_pk_tuple())

    def _to_pk_tuple(self):
        return tuple(sorted(self._to_pk().items()))

    def __lt__(self, other: "ObjBase"):
        return self._to_pk_tuple() < other._to_pk_tuple()

    def __init_subclass__(cls, **_kws):
        # you must set these in the base class
        assert cls._pk, "All classes must have a _pk"

    def __init__(self, **kws):
        """Override this to control initialization, generally calling it *after* you do your own init."""
        self._meta = ObjMeta()
        self._meta.lock = RLock()
        self._check_kws(kws)
        self._meta.new = True

    def _save_pk(self):
        self._meta.pk = self._to_pk()
        self._meta.restore = self._to_db()

    @classmethod
    def _from_db(cls, dct):
        """Override this if you want to change how deserialization works."""
        return cls(**dct)

    @classmethod
    def _from_db_not_new(cls, dct):
        """Override this if you want to change how deserialization works."""
        ret = cls._from_db(dct)
        ret._meta.new = False
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

    def _update(self, dct):
        self.__dict__.update(dct)

    def _bind(self, table: "Table" = None, manager: "Omen" = None):
        if table is None:
            table = manager[self._table_type]
        self._meta.table = table

    def _to_db(self):
        """Get dict of serialized data from self."""
        ret = {}
        for k in self._table_type.field_names:
            v = getattr(self, k)
            if v is None:
                continue
            if hasattr(v, "_to_db"):
                # pylint: disable=no-member
                v = v._to_db()
            ret[k] = v
        return ret

    def _to_pk(self):
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
            if v is None:
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

    def __setattr__(self, k, v):
        if k[0] == "_":
            super().__setattr__(k, v)
            return

        if self._meta and self._meta.table and not self._meta.locked:
            raise OmenError("use with: protocol for bound objects")

        if self._meta and not hasattr(self, k):
            raise AttributeError("Attribute %s not defined" % k)
        super().__setattr__(k, v)

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
        if self._meta.pk:
            pk = self._to_pk()
            if pk != self._meta.pk:
                for k, v in self._meta.pk.items():
                    setattr(self, k, v)
                try:
                    return self._get_related()
                finally:
                    for k, v in pk.items():
                        setattr(self, k, v)
        return {}

    def _commit(self):
        cascade = self._collect_cascade() if self._cascade else {}

        if self._meta.table:
            self._save()

        for val in self.__dict__.values():
            if isinstance(val, Relation):
                val.commit(self._meta.table.manager)

        for rel, objs in cascade.items():
            for obj in objs:
                rel._link_obj(obj)

    def _remove(self):
        cascade = self._get_related() if self._cascade else {}

        if self._meta.table:
            table = self._meta.table
            table._remove(self)

        for rel, objs in cascade.items():
            for obj in objs:
                rel.remove(obj)

    def _save(self):
        need_id_field = self._need_id()
        table = self._meta.table
        if need_id_field or self._meta.new:
            table.insert(self, need_id_field)
        else:
            table.update(self)

        self._meta.new = False
        self._save_pk()

    def _rollback(self):
        pk = self._to_pk()
        db_row = self._meta.table.db_select(pk)
        if db_row:
            if len(db_row) > 1:
                raise OmenMoreThanOneError("more than 1 %s in the db" % self.__class__)
        self._update(db_row[0])

    def __enter__(self):
        if self._meta and self._meta.table:
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
