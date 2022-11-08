# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Code generation types: imported by all codegen users."""
from typing import Callable

__autodoc__ = False

from notanorm import DbType


def any_type(arg):
    """Pass-through converter."""
    # return value as python interpreted
    return eval(arg)  # pylint: disable=eval-used


any_type.__name__ = "Any"


def bool_type(arg):
    """Convert sql string to bool, this function must have the __name__ 'bool'"""
    arg = arg.lower()
    if arg == "true":
        return True
    elif arg == "false":
        return False
    else:
        raise ValueError(f"Invalid boolean value: {arg!r}")


bool_type.__name__ = "bool"


def string_type(arg):
    """Convert sql string to str, this function must have the __name__ 'str'"""
    if arg[0] in ("'", '"'):
        return arg
    else:
        # needed for ddl parser, vs sqlite parser.  todo: normalize default values
        return "'" + arg + "'"


string_type.__name__ = "str"


def default_type(typ: DbType) -> Callable:  # pylint: disable=too-many-return-statements
    """Returns a callable that converts a database default value string to the correct python type.

    Callable name must be the typename to be used.
    """
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
    # should never happen
    raise AssertionError("unknown type: %s" % typ)
