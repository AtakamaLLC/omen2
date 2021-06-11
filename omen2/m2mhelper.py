from omen2.object import ObjBase
from omen2.errors import OmenKeyError

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
    from omen2 import Omen, Table, Relation

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
        if hasattr(self._obj2, key):
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
        return (
            self._obj2 < other._obj2
            or self._obj2 == other._obj2
            and self._obj1 < other._obj1
        )

    def __gt__(self, other: "M2MMixObj"):
        return (
            self._obj2 > other._obj2
            or self._obj2 == other._obj2
            and self._obj1 > other._obj1
        )


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

    def add(self, obj_or_id: T2 = None, **kws) -> Union[T2, M2MMixObj[T1, T2]]:
        if obj_or_id is None or not isinstance(obj_or_id, (M2MMixObj, ObjBase)):
            # we have to call "get" on table 2, to get the obj
            # but we want to only use the primary keys of table 2
            # otherwise, we will fail `matches()`
            kws2 = {}
            for k in kws.copy():
                if k in self.row_type_2._pk:
                    kws2[k] = kws.pop(k)
            obj = self.table_2.get(obj_or_id, **kws2)
            if not obj:
                raise OmenKeyError("%s not found", self.table_2.table_name)
        else:
            obj = obj_or_id

        self.__resolve_where(kws, side=0, invert=False)
        self.__resolve_where(kws, side=1, obj=obj, invert=False)

        res = self.table.new(**kws)
        super().add(res)

        return M2MMixObj(res, obj)

    def select(self, _where={}, **kws) -> Iterable[Union[T2, M2MMixObj[T1, T2]]]:
        kws2 = {}
        for k in kws.copy():
            if k not in self.table_type.field_names:
                kws2[k] = kws.pop(k)

        kws3 = kws2.copy()
        for rel in super().select(_where, **kws):
            # noinspection PyUnboundLocalVariable
            self.__resolve_where(kws2, side=1, obj=rel, invert=True)
            for sub in self.table_2.select(kws2):
                if sub._matches(kws3):
                    yield M2MMixObj(sub, rel)

    def __call__(self, _id=None, **kws) -> Optional[Union[T2, M2MMixObj[T1, T2]]]:
        # noinspection PyProtectedMember
        if _id is not None:
            # if the user specifies a positional, we assume they mean the sub-table
            assert len(self.row_type_2._pk) == 1
            kws[self.row_type_2._pk[0]] = _id
        return super().__call__(**kws)

    def remove(self, obj_or_id: "ObjBase" = None, **kws):
        if not isinstance(obj_or_id, (ObjBase, M2MMixObj)) or not obj_or_id:
            # we have to call "get" to get the obj
            obj = self.get(obj_or_id, None, **kws)
            if obj is None:
                return
        else:
            obj = obj_or_id
        obj = self.select_one(**obj._to_pk())
        if obj:
            super().remove(obj._obj2)

    def __iter__(self):
        return self.select()

    def __len__(self):
        return sum(1 for _ in self.select())

    def get(self, _id=None, _default=None, **kws) -> Optional[M2MMixObj[T1, T2]]:
        """Shortcut method, you can access object by a single pk/positional id."""
        if _id is not None:
            # if you only specify an id, we assume you mean the related-table's pk
            # get-semantics for the related table will pick the right fields
            obj = self.table_2.get(_id)
            if not obj:
                return None
            # then we convert that to a where clause on the m2m table
            # and merge it in with any other keywords specified
            self.__resolve_where(kws, side=0, invert=False)
            self.__resolve_where(kws, side=1, obj=obj, invert=False)
        return self.select_one(**kws) or _default
