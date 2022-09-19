# [omen2](omen2.md).table
Omen2: Table class and supporting types


[(view source)](https://github.com/atakamallc/omen2/blob/master/omen2/table.py)
## ObjCache(Selectable) [T=ObjBase]
Omen2 object cache: same interface as table, but all objects are preloaded.


#### .reload(self)
Reload the objects in the cache from the db.

#### .select(self, \_where={}, **kws) -> Iterable[~T]
Read objects from the cache.


## Table(Selectable) [T=ObjBase]
Omen2: Table base class from which tables are derived.


#### .\_\_init\_\_(self, mgr:'Omen')
Bind table to omen manager.

#### .add(self, obj:~U) -> ~U
Insert an object into the db

#### .count(self, \_where={}, **kws) -> int
Return count of objs matching where clause.

#### .db\_insert(self, obj:~T, id\_field)
Update the db + cache from object.

#### .db\_select(self, where)
Call select on the underlying db, given a where dict of keys/values.

#### .db\_select\_gen(self, where, order\_by=None)
Call select_gen on the underlying db, given a where dict of keys/values.

#### .db\_upsert(self, obj:~T, id\_field, up\_fds)
Upsert the db + cache from object.

#### .new(self, *a, **kw) -> ~T
Convenience function to create a new row and add it to the db.

Equivalent to: table.add(Object(*a, **kw))



#### .remove(self, obj:'ObjBase'=None, **kws)
Remove an object from the db.

#### .remove\_all(self, **kws)
Remove all matching objects from the db.

#### .select(self, \_where={}, \_order\_by=None, **kws) -> Iterable[~T]
Read objects of specified class.

Specify _order_by="field" or ["field1 desc", "field2"] to sort the results.


#### .transaction(self)
Use in a with block to enter a transaction on this table only.

#### .update(self, obj:~T, keys:Iterable[str])
Update object db + cache

#### .upsert(self, *a, \_insert\_only=None, **kw) -> ~T
Update row in db if present, otherwise, insert row.

table.upsert(Object(...))

or

table.upsert(key2=val1, key2=val2)

Note: If using the keyword-version of this function, all values that
      are not indicated by the keywords will retain the values of the existing row.



## TxStatus(Enum)
Status of objects in per-thread transaction cache.
UPDATE: object was edited
ADD: object was added
REMOVE: object was removed




