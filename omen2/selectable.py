from typing import TypeVar, Generic, Optional, Iterable, TYPE_CHECKING, Type

from omen2.errors import OmenMoreThanOneError, OmenKeyError

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from omen2 import ObjBase

T = TypeVar("T", bound="ObjBase")


# noinspection PyDefaultArgument
class Selectable(Generic[T]):
    # pylint: disable=dangerous-default-value, protected-access

    row_type: Type["T"]

    # noinspection PyProtectedMember
    def get(self, _id=None, _default=None, **kws) -> Optional[T]:
        """Shortcut method, you can access object by a single pk/positional id."""
        if _id is not None:
            assert not kws and len(self.row_type._pk) == 1
            return self._get_by_id(_id) or _default
        return self.select_one(**kws) or _default

    def _get_by_id(self, _id):
        assert len(self.row_type._pk) == 1
        kws = {self.row_type._pk[0]: _id}
        return self.select_one(**kws)

    def __contains__(self, item) -> bool:
        # noinspection PyTypeChecker
        if isinstance(item, self.row_type):
            # noinspection PyProtectedMember
            return self.select_one(_where=item._to_pk()) is not None
        return self._get_by_id(item) is not None

    def __call__(self, _id=None, **kws) -> Optional[T]:
        if _id is not None:
            # noinspection PyProtectedMember
            assert len(self.row_type._pk) == 1
            # noinspection PyProtectedMember
            kws[self.row_type._pk[0]] = _id
        ret = self.select_one(**kws)
        if ret is None:
            raise OmenKeyError("%s in %s" % (kws, self.__class__.__name__))
        return ret

    def __iter__(self):
        return self.select()

    def select_one(self, _where={}, **kws) -> Optional[T]:
        """Return one row, None, or raises an OmenMoreThanOneError."""
        itr = self.select(_where, **kws)
        return self._return_one(itr)

    @staticmethod
    def _return_one(itr):
        try:
            one = next(itr)
        except StopIteration:
            return None

        try:
            next(itr)
            raise OmenMoreThanOneError
        except StopIteration:
            return one

    def select(self, _where={}, **kws) -> Iterable[T]:
        """Read objects of specified class."""
        raise NotImplementedError
