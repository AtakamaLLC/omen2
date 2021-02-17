class OmenError(RuntimeError):
    pass


class OmenMoreThanOneError(OmenError):
    pass


class OmenNoPkError(OmenError, ValueError):
    pass
