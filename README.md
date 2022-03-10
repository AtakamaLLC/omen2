Simple library that makes working with classes and databases more convenient in python.

`omen2` will take a set of classes, and a database, and link them together.

omen2 allows the user to:

 - serialize created objects to the db
 - query the db and return objects, not rows
 - query for related objects
 - access/lock/update singleton objects across multiple threads
 - roll back changes on exceptions
 - create objects not-bound to the db, and later, bind them
 - cache objects in use, flexible cache-control

omen2 is not fully flexible with regards to database structure:

 - related tables must have primary keys

[autodoc documentation](docs/omen2.md)

```python
from notanorm import SqliteDb
from omen2 import Omen

class MyOmen(Omen):
    version = 2

    @staticmethod
    def schema(version):
        # use an omen2-compatible schema, which is a semicolon-delimited sqlite-compatible create statement
        return """create table cars(id integer primary key, color text not null, gas_level double default 1.0);
                  create table doors(carid integer, type text, primary key (carid, type));"""
        
        # or, just return a list of type-annotated classes derived from ObjBase

        # or, don't have one at all, it's ok

# you don't have to codegen, you can also just derive from omen2.ObjBase
# but you have to match your database system to the model one way or another
# either manual, codegen, or dbgen
MyOmen.codegen()

# assuming this is example.py
import example_gen as gen_objs

# every table has a row_type, you can derive from it
class Car(gen_objs.cars_row):
    def __init__(self, color="black", **kws):
        self.not_saved_to_db = "some thing"
        self.doors = gen_objs.doors_relation(self, kws.pop("doors", None), carid=lambda: self.id)
        super().__init__(color=color, **kws)

    @property
    def gas_pct(self):
        # read only props are fine
        return self.gas_level * 100


# if you're using code generation, every db table has a type, you can derive from it
class Cars(gen_objs.cars):
    # feel free to redefine the row_type used
    row_type = Car


db = SqliteDb(":memory:")

mgr = MyOmen(db, cars=Cars)

# there's always a mapping from table class to instance
# so Omen knows what classes are in charge of what tables
mgr.cars = mgr[Cars]

# fine too (or stick in init)
mgr.cars = Cars(self)

# by default, you can always iterate on tables
assert mgr.cars.count() == 0

car = Car()         # creates a default car (black, full tank)
car.color = "red"
car.gas_level = 0.5

# you don't need to create a class, if you use the code-generated one
car.doors.add(gen_objs.doors_row(type="a"))
car.doors.add(gen_objs.doors_row(type="b"))
car.doors.add(gen_objs.doors_row(type="c"))
car.doors.add(gen_objs.doors_row(type="d"))
mgr.cars.add(car)

# cars have ids, generated by the db
assert car.id

mgr.cars.add(Car(color="red", gas_level=0.3, doors=[gen_objs.doors_row(type=str(i)) for i in range(4)]))

assert sum(1 for _ in mgr.cars.select(color="red")) == 2    # 2

print("cars:", list(mgr.cars.select(color="red")))

car = mgr.cars.select_one(color="red", gas_level=0.3)

with car:
    car.gas_level = 0.9

assert len(car.doors) == 4
```

To run codegen manually, rather than "inline", you can run: `omen2-codegen my.package.MyClassName`.

Commiting this file, and running this as a git-hook on any change is a useful pattern.
