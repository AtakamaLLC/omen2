# pylint: disable=protected-access

import logging
from threading import RLock
from typing import TypeVar, Type, TYPE_CHECKING, Optional, Tuple, Set

from attr import dataclass

from omen2.errors import OmenError
from omen2.relation import Relation

if TYPE_CHECKING:
    from omen2.omen import Table, Omen

log = logging.getLogger(__name__)


class Changes(dict):
    pass

class ObjLockError(OmenError):
    pass

def to_db(typ, dat):
    return dat

@dataclass
class ObjMeta:
    lock = RLock()
    locked = False
    changes = Changes()
    table: Optional["Table"] = None


# noinspection PyCallingNonCallable
class ObjBase:
    __registry = set()
    __frozen = False
    __lock = None

    # objects have 3 non-private attributes
    _pk: Set[str] = ()
    _meta: ObjMeta = None
    table_type: Type["Table"]

    def __eq__(self, obj):
        return obj.to_pk() == self.to_pk()

    def __hash__(self):
        return hash(self._to_pk_tuple())

    def _to_pk_tuple(self):
        return tuple(sorted(self.to_pk().items()))

    @classmethod
    def __init_subclass__(cls):
        ObjBase.__registry.add(cls)

    def __init__(self, **kws):
        self._meta = ObjMeta()
        self._meta.lock = RLock()
        self._meta.changes = Changes()

        assert self._pk

        self.check_kws(kws)

    def check_kws(self, dct):
        for k in dct:
            if k not in self.__dict__:
                raise AttributeError("%s not a valid column in %s", k, self.__class__.__name__)

    def matches(self, dct):
        for k, v in dct.items():
            if getattr(self, k) != v:
                return False
        return True

    @classmethod
    def from_db(cls, dct):
        return cls(**dct)

    def _bind(self, table: "Table"=None, manager: "Omen" = None):
        if table is None:
            table = getattr(manager, self.table_type.table_name)
        self._meta.table = table

    def to_dict(self, keys=None):
        """Get dict of serialized data from self."""
        ret = {}
        for k, v in self.__dict__.items():
            if k[0] == "_":
                continue
            if k not in self.table_type.field_names:
                continue
            if keys and k not in keys:
                continue
            if hasattr(v, "to_db"):
                v = v.to_db()
            if v is not None:
                ret[k] = v
        return ret

    def to_pk(self):
        """Get dict of serialized data from self, but pk elements only."""
        return self.to_dict(self._pk)

    @classmethod
    def from_row(cls: Type['ObjType'], row) -> 'ObjType':
        """Make new object from dict of serialized data."""
        return cls.from_db(row.__dict__)

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
        self.__dict__.update(self._meta.changes.__dict__)
        if self._meta.table:
            self._save()
        for val in self.__dict__.values():
            if isinstance(val, Relation):
                val.commit(self._meta.table.manager)

    def _save(self):
        self._meta.table.update(self)

    def _rollback(self):
        self._meta.changes.clear()

    def __enter__(self):
        if not self._meta.table:
            raise OmenError("with: protocol should not be used for unbound objects")
        self._meta.lock.acquire()
        self._meta.locked = True
        return self

    def __exit__(self, typ, val, ex):
        if typ:
            self._meta.changes.clear()
        else:
            self._commit()

        self._meta.locked = False
        self._meta.lock.release()


ObjType = TypeVar('ObjType', bound=ObjBase)