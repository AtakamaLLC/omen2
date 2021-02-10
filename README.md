Simple library that makes working with classes and databases more convenient in python.

`omen2` will take a set of classes, and a database, and link them together
with migration support.

omen2 allows the user to:

 - serialize created objects to the db
 - query the db and return objects, not rows
 - query for related objects

omen2 is not fully flexible with regards to database structure:

 - related tables must have primary keys


```python
from omen2 import SqliteDb 
from omen2 import Omen

class MyOmen(Omen):
    version = 2

    @staticmethod
    def schema(version):
        # if you want to test migration, store old versions, and return them here
        assert version == 2

        # use an omen2-compatible schema, which is a semicolon-delimited create statement
        return "create table cars(id integer primary key, color text not null, gas_level real default 1.0);
                create table doors(carid integer, type text);"

    def migrate(db, version):
        # you should create a migration for each version
        assert False

# the "relation" type is used to define a relation
class Hinge(MyOmen.doors.relation):
    def hinge_info(self):
        return "ok to add stuff here"

# every table has a row_type, you can derive from it
class Car(MyOmen.cars.row_type):
    # shortcut for: car.__manager__.doors.select(id=self.id)
    doors: Hinge[Car.id]

    def __init__(self, **kws):
        self.not_saved_to_db = "some thing"
        super().__init__(**kws)

    def __create__(self):
        # called when a new, empty car is created, but not when one is loaded from a row
        # defaults from the db should be preloaded
        assert self.gas_level == 1.0

        # but you can add your own
        self.color = "default black"
        
    @property
    def gas_pct(self):
        # read only props are fine
        return self.gas_level * 100

    def __validate__(self):
        # called before saving
        assert len(self.doors) == 4

# every db table has a type, you can derive from it
class Cars(MyOmen.cars):
    # feel free to redefine the row_type used
    row_type = Car
 

fname = "test.txt"
db = SqliteDb(fname)

# upon connection to a database, this will do migration, or creation as needed
mgr = MyOmen(db)

# by default, you can always iterate on tables
assert len(mgr.cars) == 0

# feel free to replace collections, or create new ones
mgr.cars = Cars(mgr)

car = Car()                 # creates a default car (black, full tank)
car.color = "red"
car.gas_level = 0.5
car.doors.insert(MyOmen.door(type="a"))
car.doors.insert(MyOmen.door(type="b"))
car.doors.insert(MyOmen.door(type="c"))
car.doors.insert(MyOmen.door(type="d"))

mgr.insert(car)             # will save to the right table

mgr.insert(Car(color="red", gas=0.3, doors=[MyOmen.door(type=str(i)) for i in range(4)]))

mgr.cars.select(color="red")                # generator
mgr.cars.count(color="red")                 # 2

car = mgr.cars.select_one(color="red", gas=0.3)

car.gas = 0.9

# serialize changes to db
car.commit()
```

