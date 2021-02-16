import keyword
import os
import sys
import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from notanorm import DbTable


class CodeGen:
    def __init__(self, module_path):
        self.path = module_path
        self.package, self.module, self.class_name = self.parse_class_path(self.path)
        self.base_cls = self.import_mod()
        self.model = self.base_cls.model

        # trivially escape all reserved words
        # if this isn't good enough, you're using some weird db column names
        for tab in self.model.values():
            for col in tab.columns:
                if keyword.iskeyword(col.name):
                    col.name = col.name + "_"

    @staticmethod
    def gen_class(out, name, dbtab: "DbTable"):
        # pylint: disable=import-outside-toplevel
        from omen2.omen import default_type

        print("class " + name + "_row(ObjBase):", file=out)

        keys = [col.name for col in dbtab.columns]
        for index in dbtab.indexes:
            if index.primary and index.fields:
                keys = index.fields

        print("    _pk = {'" + "', '".join(keys) + "'}", file=out)
        print("", file=out)
        print("    def __init__(self, *, ", file=out, end="")
        for col in dbtab.columns:
            pytype = default_type(col.typ)
            name_and_type = col.name + ": " + pytype.__name__
            print(name_and_type, file=out, end="")
            if col.default or not col.notnull:
                if col.default:
                    defval = pytype(col.default)
                else:
                    defval = None
                print(" = " + str(defval), file=out, end="")
            print(", ", file=out, end="")
        print("**kws):", file=out)

        for col in dbtab.columns:
            print("        self." + col.name + " = " + col.name, file=out)

        print("        super().__init__(**kws)", file=out)

        print(file=out)

        print(file=out)
        print("class " + name + "(Table):", file=out)
        print('    table_name = "' + name + '"', file=out)
        print("    row_type = " + name + "_row", file=out)
        print(
            "    field_names = {'"
            + "', '".join(col.name for col in dbtab.columns)
            + "'}",
            file=out,
        )
        print("\n", file=out)
        print("class " + name + "_relation(Relation[" + name + "_row]):", file=out)
        print("    table_type = " + name, file=out)

    def output_path(self, source_file=None):
        if source_file:
            path, _ = os.path.splitext(source_file)
            path += "_gen.py"
            return path
        package, module, _cls = self.parse_class_path(self.path)
        path = (package + "." + module).replace(".", "/") + "_gen.py"
        return path

    @staticmethod
    def gen_import(out):
        print(
            "from omen2 import ObjBase, Table, Relation",
            file=out,
        )
        print(
            "\n",
            file=out,
        )

    def gen_monolith(self, out):
        self.gen_import(out)

        for name, dbtab in self.model.items():
            self.gen_class(out, name, dbtab)
            print("\n", file=out)
        print(
            '__all__ = ["' + '", "'.join(name for name in self.model) + '"]', file=out
        )

    @staticmethod
    def parse_class_path(path):
        sys.path = [os.getcwd()] + sys.path
        parts = path.split(".")
        cls = parts[-1]
        module = parts[-2]
        package = ".".join(parts[0:-2])
        return package, module, cls

    def import_mod(self):
        module = importlib.import_module("." + self.module, self.package)
        return getattr(module, self.class_name)

    @staticmethod
    def generate_from_class(class_type):
        class_path = class_type.__module__ + "." + class_type.__name__
        CodeGen.generate_from_path(
            class_path, sys.modules[class_type.__module__].__file__
        )

    @staticmethod
    def generate_from_path(class_path, source_file=None):
        cg = CodeGen(class_path)
        out_path = cg.output_path(source_file)
        tmp_path = out_path + ".tmp"
        with open(tmp_path, "w") as outf:
            cg.gen_monolith(outf)
        os.replace(tmp_path, out_path)


def main():
    CodeGen.generate_from_path(sys.argv[-1])


if __name__ == "__main__":
    main()
