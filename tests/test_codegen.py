import sys

from omen2 import Omen
from omen2.codegen import CodeGen, main


def test_codegen():
    class Test(Omen):
        @classmethod
        def schema(cls, version):
            return """
                create table zappy(
                    id integer primary key, 
                    nonnull text not null,
                    defstr text not null default 'default txt', 
                    floaty double default 1.0,
                    bad_default not null default (strftime('%s','now')) ,
                    class text
                );
            """

    mod = Test.codegen()

    zap = mod.zappy_row(nonnull="val")
    assert zap.floaty == 1.0
    assert zap.class_ is None
    assert zap.defstr == "default txt"


def test_codegen_pathed():
    mod = CodeGen.generate_from_path("tests.schema.MyOmen")
    assert mod.cars
    assert mod.cars_row
    assert mod.cars_relation


def test_codegen_main():
    sys.argv = ["progname", "tests.schema.MyOmen"]
    main()
    import tests.schema_gen

    assert tests.schema_gen.cars
