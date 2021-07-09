from typing import Any

from notanorm import DbType


def any_type(arg):
    return arg


def default_type(typ: DbType) -> Any:  # pylint: disable=too-many-return-statements
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
