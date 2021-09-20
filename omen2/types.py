"""Code generation types: imported by all codegen users."""
from typing import Callable

from notanorm import DbType


def any_type(arg):
    """Pass-through converter."""
    return arg


def default_type(typ: DbType) -> Callable:  # pylint: disable=too-many-return-statements
    """Returns a callable that converts a database default value string to the correct python type."""
    if typ == DbType.ANY:
        return any_type
    if typ == DbType.INTEGER:
        return int
    if typ == DbType.FLOAT:
        return float
    if typ == DbType.TEXT:
        return str
    if typ == DbType.BLOB:
        return bytes
    if typ == DbType.BOOLEAN:
        return bool
    if typ == DbType.DOUBLE:
        return float
    raise ValueError("unknown type: %s" % typ)
