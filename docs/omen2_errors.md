# [omen2](omen2.md).errors
Omen2: Public error classes


[(view source)](https://github.com/atakamallc/omen2/blob/master/omen2/errors.py)
## OmenError(RuntimeError)
Omen base error class.



## OmenKeyError(OmenError,KeyError)
Searched-for object doesn't exist, but one is expected.



## OmenLockingError(Exception)
Deadlock detection.  If this is thrown, the system should drop out/die/fail hard.



## OmenMoreThanOneError(OmenError)
Table has more than one matching row, but only one was expected.



## OmenNoPkError(OmenError,ValueError)
Object has None in one or more of its primary key fields, and is attempted to commit to the db



## OmenRollbackError(OmenError)
If this is thrown, changes are rolled back without re-raising.



## OmenUseWithError(OmenError)
Atempting to modify an object outside of a database modification block.



