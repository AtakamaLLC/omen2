"""Code generation types: imported by all codegen users."""
from typing import Callable

from notanorm import DbType


def any_type(arg):
    """Pass-through converter."""
    # return value as python interpreted
    return eval(arg)


any_type.__name__ = "Any"


def bool_type(arg):
    arg = arg.lower()
    if arg == "true":
        return True
    elif arg == "false":
        return False
    else:
        raise ValueError(f"Invalid boolean value: {arg!r}")


bool_type.__name__ = "bool"


def string_type(arg):
    assert arg[0] == "'"
    # return with quotes
    return arg


string_type.__name__ = "str"


def default_type(typ: DbType) -> Callable:  # pylint: disable=too-many-return-statements
    """Returns a callable that converts a database default value string to the correct python type."""
    if typ == DbType.ANY:
        return any_type
    if typ == DbType.INTEGER:
        return int
    if typ == DbType.FLOAT:
        return float
    if typ == DbType.TEXT:
        return string_type
    if typ == DbType.BLOB:
        return bytes
    if typ == DbType.BOOLEAN:
        return bool_type
    if typ == DbType.DOUBLE:
        return float
    raise ValueError("unknown type: %s" % typ)
