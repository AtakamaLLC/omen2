# [omen2](omen2.md).codegen
Omen2: generate python code from a database schema.


[(view source)](https://github.com/atakamallc/omen2/blob/master/omen2/codegen.py)
## CodeGen(object)
Generate code from a database schema.


#### .__init__(self, module_path, class_type=None)
Create an omen2 codegen object.

Args:
    module_path: package.module.ClassName


#### .gen_class(out, name, dbtab:'DbTable')
Generate the derived classes for a single DBTable

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



#### .gen_import(out)
Generate import statements.

#### .gen_monolith(self, out)
Generates a single, monolithic file with all classes in one file.

#### .generate_from_class(class_type, out_path=None)
Given a class derived from omen2.Omen, generate omen2 code.

#### .generate_from_path(class_path, class_type=None, out_path=None)
Given a dotted python path name, generate omen2 code.

#### .import_generated(self, out_path)
Import the module this codegen generated.

#### .import_mod(self)
Import the module this codegen will be running on.

#### .output_path(self)
Get the codegen output path.

Example: <module-path>_gen.py


#### .parse_class_path(path)
Parse the package.module.ClassName path.


## Functions:

#### main()
Command line codegen: given a moddule path, generate code.

