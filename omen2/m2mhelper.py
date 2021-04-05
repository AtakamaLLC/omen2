from typing import (
    TypeVar,
    Iterable,
    TYPE_CHECKING,
    Type,
    Tuple,
    Union,
    Generic,
    Optional,
)

from .relation import Relation

if TYPE_CHECKING:
    from omen2 import ObjBase, Omen, Table, Relation

T1 = TypeVar("T1")
T2 = TypeVar("T2")


class M2MMixObj(Generic[T1, T2]):
    """Mixes two objects and returns the hybrid.

    The hybrid object has all the attributes of T1, and any attributes of T2 that are not in T1.

    Using "with" and comparing them does what you would expect (T1a == T1a and T1a == T2b).
    """

    __ready = False
    _obj1 = None
    _obj2 = None

    def __init__(self, obj1: T1, obj2: T2):
        self._obj1 = obj1
        self._obj2 = obj2
        self.__ready = True

    def __getattr__(self, key):
        if not hasattr(self._obj1, key):
            return getattr(self._obj2, key)
        return getattr(self._obj1, key)

    def __setattr__(self, key, val):
        if not self.__ready:
            object.__setattr__(self, key, val)
            return

        if not hasattr(self._obj1, key):
            setattr(self._obj2, key, val)
            return

        setattr(self._obj1, key, val)

    def __enter__(self):
        self._obj1.__enter__()
        self._obj2.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._obj2.__exit__(exc_type, exc_val, exc_tb)
        self._obj1.__exit__(exc_type, exc_val, exc_tb)

    def __eq__(self, other: "M2MMixObj"):
        return self._obj1 == other._obj1 and self._obj2 == other._obj2

    def __lt__(self, other: "M2MMixObj"):
        return self._obj1 < other._obj1 and self._obj2 < other._obj2

    def __gt__(self, other: "M2MMixObj"):
        return self._obj1 > other._obj1 and self._obj2 > other._obj2


# noinspection PyProtectedMember,PyDefaultArgument
class M2MHelper(Relation[Union[T2, M2MMixObj[T1, T2]]]):
    """Convenience class that defines a special case of Relationship.

    The M2MHelper returns a collection of T2, instead of a collection of T1 objects.

    If there fields not part of thw where-joins, you get a collection of mixin's:
        a union of T1 (the m2m table) and T2 (the destination table).

    For example:

        Groups -> GroupPeople -> People

    Allows you to create a collection of MixinPeople related to a instance of a Group.

    Any fields that are specific to the relation-table will show up as attribute of the Mixin.

    If there are no unique fields in the relation table, then a mixin will not be used.
    """

    # pylint: disable=protected-access, dangerous-default-value

    table_type_2: "Type[Table[T2]]" = None

    @property
    def row_type_2(self):
        return self.table_type_2.row_type

    def __init__(
        self,
        _from: "ObjBase",
        *,
        types: Tuple[Type["Table[T1]"], Type["Table[T2]"]],
        where: Tuple[dict, dict]
    ):
        self.__field_map = where
        self.__table2 = None

        # relationships use lamdas to get id's from the related table
        rel_where = where[0].copy()

        def getter_func(v):
            return lambda: getattr(_from, v)

        for k, v in rel_where.items():
            rel_where[k] = getter_func(v)

        self.table_type = types[0]
        self.table_type_2 = types[1]

        # if the number of fields in the m2m table are the same as the number of
        # clauses in th relationship, we don't need to mix-in
        self.need_mixin = len(self.table_type.field_names) > (
            len(where[0]) + len(where[1])
        )

        # always cascade m2m table deletions and pk updates
        super().__init__(_from, where=rel_where, cascade=True)

    @property
    def table_2(self):
        if not self.__table2:
            mgr: "Omen" = self._from._meta.table.manager
            self.__table2: "Table" = mgr.get_table_by_name(self.table_type_2.table_name)
            self.table_type_2 = type(self.__table2)
        return self.__table2

    def __resolve_where(
        self, resolved, *, side: int, obj: "ObjBase" = None, invert: bool
    ):
        for k, v in self.__field_map[side].items():
            if invert:
                resolved[v] = getattr(obj, k)
            else:
                if side == 0:
                    resolved[k] = getattr(self._from, v)
                else:
                    resolved[k] = getattr(obj, v)

    def add(self, obj: T2, **kws) -> Union[T2, M2MMixObj[T1, T2]]:
        self.__resolve_where(kws, side=0, invert=False)
        self.__resolve_where(kws, side=1, obj=obj, invert=False)

        res = self.table.new(**kws)
        super().add(res)

        if self.need_mixin:
            return M2MMixObj(res, obj)
        else:
            return obj

    def select(self, _where={}, **kws) -> Iterable[Union[T2, M2MMixObj[T1, T2]]]:
        kws2 = {}
        for k in kws.copy():
            if k not in self.table_type.field_names:
                kws2[k] = kws.pop(k)

        for rel in super().select(_where, **kws):
            # noinspection PyUnboundLocalVariable
            self.__resolve_where(kws2, side=1, obj=rel, invert=True)
            for sub in self.table_2.select(kws2):
                if sub._matches(kws2):
                    if self.need_mixin:
                        yield M2MMixObj(sub, rel)
                    else:
                        yield sub

    if TYPE_CHECKING:

        def get(  # pylint: disable=unused-argument, no-self-use
            self, _id=None, _default=None, **kws
        ) -> Optional[Union[T2, M2MMixObj[T1, T2]]]:
            ...

    def __call__(self, _id=None, **kws) -> Optional[Union[T2, M2MMixObj[T1, T2]]]:
        # noinspection PyProtectedMember
        if _id is not None:
            # if the user specifies a positional, we assume they mean the sub-table
            assert len(self.row_type_2._pk) == 1
            kws[self.row_type_2._pk[0]] = _id
        return super().__call__(**kws)

    def remove(self, obj: "ObjBase"):
        obj = self.select_one(**obj._to_pk())
        if obj:
            super().remove(obj._obj2)
