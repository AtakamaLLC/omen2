from typing import TypeVar, Generic, Callable, Iterable, TYPE_CHECKING, List

if TYPE_CHECKING:
    from omen2 import ObjBase, Omen, Table

T = TypeVar("T")


# noinspection PyProtectedMember,PyDefaultArgument
class Relation(Generic[T]):
    # pylint: disable=protected-access, dangerous-default-value

    table_type: "Table" = None

    def __init__(self, _from: "ObjBase", _init=None, **where):
        self._from = _from
        self._where = where
        self.__table = None
        self.__saved: List["ObjBase"] = []
        if _init:
            for ent in _init:
                if isinstance(ent, dict):
                    self.add(T(ent))
                else:
                    self.add(ent)

    def is_bound(self):
        return self._from._meta and self._from._meta.table

    @property
    def table(self):
        if not self.__table:
            mgr: "Omen" = self._from._meta.table.manager
            self.__table: "Table" = getattr(mgr, self.table_type.table_name)
        return self.__table

    def add(self, obj: "ObjBase"):
        if not self.is_bound():
            self.__saved.append(obj)
        else:
            self._link_obj(obj)
            self.table.add(obj)

    def remove(self, obj: "ObjBase"):
        self.table.remove(obj)

    def __len__(self):
        return sum(1 for _ in self.select())

    def select(self, where={}, **kws) -> Iterable[T]:
        where = {**where, **kws, **self._where}
        for k, v in where.items():
            if isinstance(v, Callable):
                where[k] = v()
        if self.is_bound():
            for obj in self.table.select(**where):
                if obj not in self.__saved:
                    yield obj
        for obj in self.__saved:
            if obj._matches(where):
                yield obj

    def _link_obj(self, obj):
        for k, v in self._where.items():
            if isinstance(v, Callable):
                v = v()
            setattr(obj, k, v)

    def commit(self, manager):
        for item in self.__saved:
            if not item._meta.table:
                item._bind(manager=manager)
            with item:
                self._link_obj(item)
        self.__saved.clear()
