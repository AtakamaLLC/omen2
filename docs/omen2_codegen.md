# [omen2](omen2.md).codegen
Omen2: generate python code from a database schema.


[(view source)](https://github.com/atakamallc/omen2/blob/master/omen2/codegen.py)
## CodeGen(object)
Generate code from a database schema.


#### .\_\_init\_\_(self, module\_path, class\_type=None)
Create an omen2 codegen object.

Args:
    module_path: package.module.ClassName


#### .gen\_class(out, name, dbtab: 'DbTable')
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



#### .gen\_import(out)
Generate import statements.

#### .gen\_monolith(self, out)
Generates a single, monolithic file with all classes in one file.

#### .generate\_from\_class(class\_type, out\_path=None)
Given a class derived from omen2.Omen, generate omen2 code.

#### .generate\_from\_path(class\_path, class\_type=None, out\_path=None)
Given a dotted python path name, generate omen2 code.

#### .import\_generated(self, out\_path)
Import the module this codegen generated.

#### .import\_mod(self)
Import the module this codegen will be running on.

#### .output\_path(self)
Get the codegen output path.

Example: <module-path>_gen.py


#### .parse\_class\_path(path)
Parse the package.module.ClassName path.


## Functions:

#### main()
Command line codegen: given a moddule path, generate code.

