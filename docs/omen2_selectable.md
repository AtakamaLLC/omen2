# [omen2](omen2.md).selectable
Generic selectable support for tables, relations and m2mhelpers.


[(view source)](https://github.com/atakamallc/omen2/blob/master/omen2/selectable.py)
## Selectable [T=ObjBase]
Generic selectable base class.


#### .count(self, \_where={}, **kws) -> int
Return count of objs matchig where clause.  Override for efficiency.

#### .get(self, \_id=None, \_default=None, **kws) -> Union[~T, NoneType]
Shortcut method, you can access object by a single pk/positional id.

#### .select(self, \_where={}, **kws) -> Iterable[~T]
Read objects of specified class.

#### .select\_one(self, \_where={}, **kws) -> Union[~T, NoneType]
Return one row, None, or raises an OmenMoreThanOneError.


