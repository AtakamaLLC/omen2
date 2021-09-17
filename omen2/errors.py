import notanorm.errors


class OmenError(RuntimeError):
    pass


class OmenMoreThanOneError(OmenError):
    pass


class OmenNoPkError(OmenError, ValueError):
    pass


class OmenKeyError(OmenError, KeyError):
    pass


class OmenUseWithError(OmenError):
    pass


class OmenRollbackError(OmenError):
    """If this is thrown, changes are rolled back without re-raising."""


class OmenLockingError(Exception):
    """Deadlock detection.  If this is thrown, the system should drop out/die/fail hard."""


IntegrityError = notanorm.errors.IntegrityError
