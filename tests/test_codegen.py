# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

import os
import sys

from omen2 import Omen
from omen2.codegen import CodeGen, main
from typing import Any, Optional


def test_many_types_sqlite(tmp_path):
    out_path = str(tmp_path / "gen.py")

    class Test(Omen):
        @classmethod
        def schema(cls, version):
            return """
                create table zappy(
                    id integer primary key,
                    nonnull text not null,
                    defstr text not null default 'default txt',
                    defstr2 text not null default "default txt2",
                    defstr3 text not null default(1.0),
                    floaty double default 1.0,
                    boolt boolean default TRuE,
                    boolf boolean default FAlSe,
                    booly boolean,
                    boolbad boolean default 'invalid_bool',
                    anyold default 4,
                    nullable default null,
                    bad_default not null default (strftime('%s','now')) ,
                    class text
                );
            """
        dialect="sqlite"

    mod = Test.codegen(out_path=out_path)

    zap = mod.zappy_row(nonnull="val")
    assert zap.floaty == 1.0
    assert zap.class_ is None
    assert zap.defstr == "default txt"
    assert zap.defstr2 == "default txt2"
    assert zap.defstr3 == "1.0"
    assert zap.boolt == True
    assert zap.boolf == False
    assert zap.booly == None
    assert zap.boolbad == None
    assert zap.anyold == 4
    assert zap.__annotations__["bad_default"] is Any
    assert zap.__annotations__["floaty"] is Optional[float]
    assert zap.__annotations__["id"] is Optional[int]
    assert zap.__annotations__["nonnull"] is str
    assert zap.__annotations__["booly"] is Optional[bool]
    assert zap.__annotations__["boolt"] is Optional[bool]
    assert zap.__annotations__["boolf"] is Optional[bool]
    assert zap.__annotations__["anyold"] is Any


def test_many_types_mysql(tmp_path):
    out_path = str(tmp_path / "gen.py")

    # tests some edge cases with defaults showing up differently depending on dialect
    class Test(Omen):
        @classmethod
        def schema(cls, version):
            return """
                create table zappy(
                    id integer primary key,
                    `defstr` text not null default 'default txt',
                    `floaty` double default 1.0,
                    `group` text not null,
                );
            """
        dialect = "mysql"

    mod = Test.codegen(out_path=out_path)

    zap = mod.zappy_row(group="val")
    assert zap.floaty == 1.0
    assert zap.defstr == "default txt"
    assert zap.group == "val"
    assert zap.__annotations__["floaty"] is Optional[float]
    assert zap.__annotations__["id"] is Optional[int]


def test_codegen_dialect(tmp_path):
    out_path = str(tmp_path / "gen.py")

    class Test(Omen):
        @classmethod
        def schema(cls, version):
            return """
                create table zappy(
                    id integer auto_increment primary key,
                    floaty float default 1.0,
                    doubly double default 1.0,
                );
            """

        dialect = "mysql"

    mod = Test.codegen(out_path=out_path)

    zap = mod.zappy_row()
    assert zap.floaty == 1.0
    assert zap.__annotations__["doubly"] is Optional[float]
    assert zap.__annotations__["floaty"] is Optional[float]
    assert zap.__annotations__["id"] is Optional[int]


def test_codegen_pathed(tmp_path):
    p = tmp_path / "gen.py"
    mod = CodeGen.generate_from_path("tests.schema.MyOmen", out_path=str(p))
    assert mod.cars
    assert mod.cars_row
    assert mod.cars_relation
    sys.modules.pop(mod.__name__)


# noinspection PyUnresolvedReferences
def test_codegen_main():
    sys.modules.pop("tests", None)
    sys.argv = ["progname", "tests.schema.MyOmen"]
    main()
    import tests.schema_gen

    assert tests.schema_gen.cars
    os.unlink(tests.schema_gen.__file__)
