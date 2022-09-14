# [omen2](omen2.md).object
Omen2 object and associated classes.


[(view source)](https://github.com/atakamallc/omen2/blob/master/omen2/object.py)
## CustomType(object)
Derive from this type so that track-changes works with your custom object.



## ObjBase(object)
Object base class, from which all objects are derived.


#### .\_\_init\_\_(self, **kws)
Override this to control initialization, generally calling it *after* you do your own init.


## ObjMeta(object)
Object private metadata containing the bound table, a lock, and other flags.



