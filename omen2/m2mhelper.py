# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Many-to-many relationship helper.   Provides nice syntax for interacting with m2m relationships."""

from omen2.object import ObjBase
from omen2.errors import OmenKeyError

from typing import (
    TypeVar,
    TYPE_CHECKING,
    Type,
    Tuple,
    Union,
    Generic,
    Optional,
    List,
    Generator,
)

from .relation import Relation

if TYPE_CHECKING:
    from omen2 import Omen, Table, Relation

T1 = TypeVar("T1", bound="ObjBase")
T2 = TypeVar("T2", bound="ObjBase")


class M2MMixObj(Generic[T1, T2]):
    """Mixes two objects and returns the hybrid.

    The hybrid object has all the attributes of T1, and any attributes of T2 that are not in T1.

    Using "with" and comparing them does what you would expect (T1a == T1a and T1a == T2b).
    """

    # pylint: disable=protected-access

    __ready = False
    _obj1 = None
    _obj2 = None

    def __init__(self, obj1: T1, obj2: T2):
        self._obj1 = obj1
        self._obj2 = obj2
        self.__ready = True

    def __repr__(self):
        return self.__class__.__name__ + repr((self._obj1, self._obj2))

    def __getattr__(self, key):
        if hasattr(self._obj1, key):
            return getattr(self._obj1, key)
        return getattr(self._obj2, key)

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
        ret = (
            self._obj1 < other._obj1
            or self._obj1 == other._obj1
            and self._obj2 < other._obj2
        )
        return ret


ROW_TYPE = Union[T2, M2MMixObj[T1, T2]]


# noinspection PyProtectedMember,PyDefaultArgument
class M2MHelper(Relation[ROW_TYPE]):
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
        """Row type of the related table."""
        return self.table_type_2.row_type

    def __init__(
        self,
        _from: "ObjBase",
        *,
        types: Tuple[Type["Table[T1]"], Type["Table[T2]"]],
        where: Tuple[dict, dict]
    ):
        """
        A "where_dict" is a dictionary describing the relationship:
        For example:
            self.Peeps = M2MHelper(types=(GroupPeeps, Peep), where=({"group_id": "id"}, "peep_id": "id"}))
        Args:
            _from:  Source object
            types:  tuple(M2M Table Type, Related Table Type)
            where:  tuple(where_dict for self, where_dict for related)
        """
        self.__saved: List[M2MMixObj] = []
        self.__field_map = where
        self.__table2: Optional["Table[T2]"] = None

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
    def table_2(self) -> Optional["Table[T2]"]:
        """Table-instance of the related table.

        Memoized getter/shortcut.
        """
        if not self.__table2:
            if not self._from._is_bound:
                return None
            mgr: "Omen" = self._from._table.manager
            self.__table2 = mgr.get_table_by_name(self.table_type_2.table_name)
            self.table_type_2 = type(self.__table2)
        return self.__table2

    def __resolve_where(
        self, resolved, *, side: int, obj: "ObjBase" = None, invert: bool
    ):
        """Convert keywords between relation table and the related tables."""
        for k, v in self.__field_map[side].items():
            if invert:
                resolved[v] = getattr(obj, k)
            else:
                if side == 0:
                    resolved[k] = getattr(self._from, v)
                else:
                    resolved[k] = getattr(obj, v)

    # pylint: disable=arguments-renamed
    def add(self, obj_or_id: T2 = None, **kws) -> ROW_TYPE:
        """Add a member of the m2m list, with extra kws for the m2m row."""

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
                raise OmenKeyError("%s not found" % self.table_2.table_name)
        else:
            obj = obj_or_id

        self.__resolve_where(kws, side=0, invert=False)
        self.__resolve_where(kws, side=1, obj=obj, invert=False)

        if not self.table_2:
            res = self.table_type.row_type(**kws)
            new_mix = M2MMixObj(res, obj)
            self.__saved.append(new_mix)
            return new_mix

        res = self.table.new(**kws)
        super().add(res)

        return M2MMixObj(res, obj)

    def select(self, _where={}, **kws) -> Generator[ROW_TYPE, None, None]:
        """Select a member of the m2m list.

        Returns mixin objects that represents the relation.
        """
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
                    yield M2MMixObj(rel, sub)

        for mix in self.__saved:
            if mix._obj1._matches(kws):
                if mix._obj2._matches(kws2):
                    yield mix

    def __call__(self, _id=None, **kws) -> ROW_TYPE:
        """Grab a specific entry by primary key or raise an error."""
        # noinspection PyProtectedMember
        if _id is not None:
            # if the user specifies a positional, we assume they mean the sub-table
            assert len(self.row_type_2._pk) == 1
            kws[self.row_type_2._pk[0]] = _id
        return super().__call__(**kws)

    def remove(  # pylint: disable=arguments-renamed
        self, obj_or_id: T2 = None, **kws
    ):
        """Remove a specific entry by primary key or raise an error."""
        if not isinstance(obj_or_id, (ObjBase, M2MMixObj)) or not obj_or_id:
            # we have to call "get" to get the obj
            obj = self.get(obj_or_id, None, **kws)
            if obj is None:
                return
        elif isinstance(obj_or_id, ObjBase):
            obj = self.get(**obj_or_id._to_pk())
            if obj is None:
                return
        else:
            obj = obj_or_id
        super().remove(obj._obj1)

    def __len__(self):
        return sum(1 for _ in self.select())

    def get(self, _id: T2 = None, _default=None, **kws) -> Optional[ROW_TYPE]:
        """Shortcut method, you can access object by a single pk/positional id."""
        if _id is not None:
            # if you only specify an id, we assume you mean the related-table's pk
            # get-semantics for the related table will pick the right fields
            self.__resolve_where(kws, side=0, invert=False)

            if self.table_2:
                if not isinstance(_id, ObjBase):
                    # grab by id from table 2
                    obj = self.table_2.get(_id)
                    if not obj:
                        return None
                else:
                    # i'm already a table 2 instance
                    obj = _id
                # then we convert that to a where clause on the m2m table
                # and merge it in with any other keywords specified
                self.__resolve_where(kws, side=1, obj=obj, invert=False)
            else:
                if isinstance(_id, ObjBase):
                    kws.update(_id._to_pk())
                elif len(self.table_type_2.row_type._pk) == 1:
                    kws[self.table_type_2.row_type._pk[0]] = _id
        return self.select_one(**kws) or _default

    def __contains__(self, item):
        return self.get(item) is not None

    def commit(self, manager=None):
        """Bind/add any items were added while I was unbound.

        # TODO: relation-with-blocks that track changes, just like their parents.
        """
        if not manager:
            manager = self.table.manager
        for item in self.__saved:
            if not item._obj1._is_bound:
                item._obj1._bind(manager=manager)
                item._obj1._commit()
            if not item._obj2._is_bound:
                item._obj2._bind(manager=manager)
                item._obj2._commit()
            self._link_obj(item)
        self.__saved.clear()
