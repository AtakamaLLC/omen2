from typing import TypeVar, Generic, TYPE_CHECKING

if TYPE_CHECKING:
    from omen2 import ObjBase

from omen2.errors import OmenMoreThanOneError

T = TypeVar("T", bound="ObjBase")


class Selectable(Generic[T]):
    row_type: "ObjBase"

    def get(self, _id=None, _default=None, **kws):
        """Shortcut method, you can access object by a single pk/positional id."""
        if _id is not None:
            assert len(self.row_type._pk) == 1
            kws[self.row_type._pk[0]] = _id
        return self.select_one(**kws) or _default

    def __iter__(self):
        return self.select()

    def select_one(self, where={}, **kws):
        """Return one row, None, or raises an OmenMoreThanOneError."""
        itr = self.select(where, **kws)
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

    def select(self, where={}, **kws):
        """Read objects of specified class."""
        raise NotImplementedError
