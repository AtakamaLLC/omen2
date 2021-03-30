import notanorm.errors


class OmenError(RuntimeError):
    pass


class OmenMoreThanOneError(OmenError):
    pass


class OmenDuplicateObjectError(OmenError):
    pass


class OmenNoPkError(OmenError, ValueError):
    pass


IntegrityError = notanorm.errors.IntegrityError
