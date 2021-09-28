"""Omen2: Public error classes"""

import notanorm.errors


class OmenError(RuntimeError):
    """Omen base error class."""


class OmenMoreThanOneError(OmenError):
    """Table has more than one matching row, but only one was expected."""


class OmenNoPkError(OmenError, ValueError):
    """Object has None in one or more of its primary key fields, and is attempted to commit to the db"""


class OmenKeyError(OmenError, KeyError):
    """Searched-for object doesn't exist, but one is expected."""


class OmenUseWithError(OmenError):
    """Atempting to modify an object outside of a database modification block."""


class OmenRollbackError(OmenError):
    """If this is thrown, changes are rolled back without re-raising."""


class OmenLockingError(Exception):
    """Deadlock detection.  If this is thrown, the system should drop out/die/fail hard."""


class IntegrityError(OmenError, notanorm.errors.IntegrityError):
    """Attempt to add dup primary key."""
