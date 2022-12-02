# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Omen2: generate python code from a database schema."""
import argparse
import keyword
import os
import sys
import importlib
import importlib.util
import logging as log
from types import ModuleType

from notanorm import DbTable, DbCol

from omen2.types import default_type, any_type


class CodeGen:
    """Generate code from a database schema."""

    def __init__(self, module_path, class_type=None):
        """Create an omen2 codegen object.

        Args:
            module_path: package.module.ClassName
        """
        self.path = module_path
        self.package, self.module, self.class_name = self.parse_class_path(self.path)
        if self.module == "__main__":
            self.module, _ = os.path.splitext(
                os.path.basename(
                    sys.modules["__main__"].__file__  # pylint: disable=no-member
                )
            )
        self.base_cls = class_type or self.import_mod()
        self.model = self.base_cls.model

        # trivially escape all reserved words
        # if this isn't good enough, you're using some weird db column names
        tab: DbTable
        for name, tab in list(self.model.items()):
            new_cols = []
            need_tab = False
            for col in tab.columns:
                new_coldef = col._asdict()
                need_col = False
                if keyword.iskeyword(col.name):
                    new_coldef["name"] = col.name + "_"
                    need_col = True
                if need_col:
                    new_col = DbCol(**new_coldef)
                    need_tab = True
                else:
                    new_col = col
                new_cols.append(new_col)
            if need_tab:
                new_tab = DbTable(columns=tuple(new_cols), indexes=tab.indexes)
                self.model[name] = new_tab

    @staticmethod
    def gen_class(out, name, dbtab: "DbTable"):
        """Generate the derived classes for a single DBTable

        Args:
            out: file stream
            name: table name
            dbtab: table model

        Example:

            class cars_row(ObjBase):
                id: int
                color: str
                _pk = ("id", )
                def __init__(self, id, color: str = "green"):
                    self.id = id
                    self.color = color

        """

        # *** ROW DEFINITION ***
        print("class " + name + "_row(ObjBase):", file=out)

        # keys is the set of fields to be used for the primary key
        # if there is no primary key, then we use "all columns"
        keys = [col.name for col in dbtab.columns]
        for index in dbtab.indexes:
            if index.primary and index.fields:
                keys = index.fields

        for col in dbtab.columns:
            pytype = default_type(col.typ)
            typename = pytype.__name__
            if not col.notnull and pytype is not any_type:
                typename = "Optional[%s]" % typename
            print("    %s: %s" % (col.name, typename), file=out)

        # _pk is a class-variable
        print(
            "    _pk = ('"
            + "', '".join([k if type(k) is str else k.name for k in keys])
            + "', )",
            file=out,
        )
        print("", file=out)

        # generate an init statement for the new class
        print("    def __init__(self, *, ", file=out, end="")
        for col in dbtab.columns:
            pytype = default_type(col.typ)
            name_and_type = col.name + ": " + pytype.__name__
            # we're in the init parameter line: `color: str`
            print(name_and_type, file=out, end="")

            if col.default is not None or not col.notnull:
                # derive default value from the db default value
                if col.default is not None:
                    try:
                        # check valid python
                        defval = pytype(str(col.default))  # pylint: disable=eval-used
                    except (ValueError, NameError):
                        # no way to generate a default value for some stuff
                        defval = None
                        log.warning(
                            "not generating python default for %s.%s=%s",
                            name,
                            col.name,
                            col.default,
                        )
                else:
                    defval = None
                # double check
                eval(str(defval))  # pylint: disable=eval-used
                # finishing one parameter: = "green"
                print(" = " + str(defval), file=out, end="")
            # comma between params
            print(", ", file=out, end="")
        # extra kws pass thru
        print("**kws):", file=out)

        # from above: self.color = color
        for col in dbtab.columns:
            print("        self." + col.name + " = " + col.name, file=out)

        # call to super init
        print("        super().__init__(**kws)", file=out)

        # newline
        print(file=out)

        # other class-level variables
        print(
            name
            + "_row_type_var = TypeVar('"
            + name
            + "_row_type_var', bound="
            + name
            + "_row)",
            file=out,
        )

        # *** TABLE DEFINITION ***
        print(file=out)
        print("class " + name + "(Table[" + name + "_row_type_var]):", file=out)
        print('    table_name = "' + name + '"', file=out)
        print("    row_type = " + name + "_row", file=out)
        print(
            "    field_names = {'"
            + "', '".join(col.name for col in dbtab.columns)
            + "'}",
            file=out,
        )

        # *** RELATION DEFINITION ***
        print("\n", file=out)
        print("class " + name + "_relation(Relation[" + name + "_row]):", file=out)
        print("    table_type = " + name, file=out)

    def output_path(self):
        """Get the codegen output path.

        Example: <module-path>_gen.py
        """
        base_path = sys.modules[self.base_cls.__module__].__file__
        path, _ = os.path.splitext(base_path)
        path = path + "_gen.py"
        return path

    @staticmethod
    def gen_import(out):
        """Generate import statements."""
        print(
            "from omen2 import ObjBase, Table, Relation, any_type\n"
            "from typing import TypeVar, Optional, Any\n",
            file=out,
        )

    def gen_monolith(self, out):
        """Generates a single, monolithic file with all classes in one file."""
        self.gen_import(out)

        for name, dbtab in self.model.items():
            self.gen_class(out, name, dbtab)
            print("\n", file=out)
        print(
            '__all__ = ["' + '", "'.join(name for name in self.model) + '"]', file=out
        )

    @staticmethod
    def parse_class_path(path):
        """Parse the package.module.ClassName path."""
        sys.path = [os.getcwd()] + sys.path
        parts = path.split(".")
        cls = parts[-1]
        module = parts[-2]
        package = ".".join(parts[0:-2])
        return package, module, cls

    def import_mod(self):
        """Import the module this codegen will be running on."""
        pack_mod = ".".join(n for n in (self.package, self.module) if n)
        if pack_mod in sys.modules:
            module = sys.modules[pack_mod]
        else:
            module = "." + self.module if self.package else self.module
            module = importlib.import_module(module, self.package)
        return getattr(module, self.class_name)

    def import_generated(self, out_path):
        """Import the module this codegen generated."""
        gen_name = self.module + "_gen"
        module_name = self.package + "." + gen_name if self.package else gen_name
        with open(out_path, "r", encoding="utf8") as f:
            code = compile(f.read(), out_path, "exec")
            module = ModuleType(module_name, "generated from %s" % self.module)
            module.__file__ = out_path
            exec(code, module.__dict__)  # pylint: disable=exec-used
            sys.modules[module_name] = module
        if self.package:
            parent = importlib.import_module(self.package)
            setattr(parent, gen_name, module)
        return module

    @staticmethod
    def generate_from_class(class_type, out_path=None):
        """Given a class derived from omen2.Omen, generate omen2 code."""
        class_path = class_type.__module__ + "." + class_type.__name__
        return CodeGen.generate_from_path(class_path, class_type, out_path)

    @staticmethod
    def generate_from_path(class_path, class_type=None, out_path=None):
        """Given a dotted python path name, generate omen2 code."""
        cg = CodeGen(class_path, class_type)

        dest_path = out_path or cg.output_path()
        tmp_path = dest_path + ".tmp"
        with open(tmp_path, "w", encoding="utf8") as outf:
            cg.gen_monolith(outf)

        os.replace(tmp_path, dest_path)

        return cg.import_generated(dest_path)


def main():
    """Command line codegen: given a moddule path, generate code."""
    parser = argparse.ArgumentParser(description="Generate omen2 database-linked code")
    parser.add_argument(
        "module",
        help="Python import path for a module contains a class derived from omen2.Omen",
    )
    parser.add_argument("--out", "-o", help="Output file path", action="store")
    args = parser.parse_args()
    CodeGen.generate_from_path(args.module, out_path=args.out)


if __name__ == "__main__":
    main()
