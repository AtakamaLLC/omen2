# pylint: disable=protected-access

import logging
from threading import RLock

log = logging.getLogger(__name__)


class Changes:
    pass


def to_db(typ, dat):
    return dat


def from_db(typ, dat):
    return dat


class Base:
    __table_name__: str
    __frozen = False
    __registry__ = set()

    @classmethod
    def __init_subclass__(cls):
        Base.__registry__.add(cls)

    def __init__(self, **kws):
        self.__lock = RLock()
        self.__changes = Changes()

        self._update(kws)
        self._db = None
        self._new = True
        self.__frozen = True

    def to_dict(self):
        """Get dict of serialized data from self."""
        ret = {}
        for k, typ in self.__annots().items():
            if not typ.related and not typ.read_only and not typ.collection:
                ret[k] = to_db(typ, getattr(self, k))

    @classmethod
    def from_dict(cls: 'Base', data) -> 'Base':
        """Make new object from dict of serialized data."""
        ret = cls()
        ret._update(data)
        return ret

    @classmethod
    def from_row(cls: 'Base', row) -> 'Base':
        """Make new object from dict of serialized data."""
        ret = cls.from_dict(row.__dict__)
        ret._new = False
        return ret

    def _bind(self, db):
        self._db = db

    def __setattr__(self, k, v):
        annot = self.__annots().get(k)
        if annot:
            if annot.prop:
                annot.prop.__set__(self.__changes, v)
            else:
                self.__changes.__dict__[k] = v
        else:
            if self.__frozen and not hasattr(self, k):
                raise AttributeError("Attribute %s not defined" % k)
            super().__setattr__(k, v)

    def _update(self, data):
        for k, typ in self.__annots().items():
            setattr(self, k, from_db(typ, data[k]))

    @classmethod
    def __annots(cls):
        return cls.__annots__

    def _commit(self):
        """Commit changes to main dict, and save to db, if any."""
        self.__dict__.update(self.__changes.__dict__)
        for val in self.__dict__.values():
            if hasattr(val, "_commit"):
                val._commit()
        if self._db:
            self._save()

    def _save(self):
        if self._new:
            self._db.insert(self.__table_name__, self.to_dict())
        else:
            self._db.update(self.__table_name__, self.to_dict())

    def _rollback(self):
        self.__changes.clear()

    def __enter__(self):
        self.__lock.acquire()

    def __exit__(self, typ, val, ex):
        if typ:
            self._commit()
        else:
            self._rollback()

        self.__lock.release()
