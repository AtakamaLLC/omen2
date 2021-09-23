import atexit
import os
import tempfile
from contextlib import suppress

from omen2 import Omen, ObjBase


_temp_paths = []


def temp_path():
    path = tempfile.NamedTemporaryFile(delete=False).name
    _temp_paths.append(path)
    return path


def _cleanup_temp():
    for file in _temp_paths:
        with suppress(FileNotFoundError):
            os.unlink(file)


atexit.register(_cleanup_temp)


class MyOmen(Omen):
    version = 2

    @classmethod
    def schema(cls, version):
        # if you want to test migration, store old versions, and return them here
        assert version == 2

        # use a notanorm-compatible schema
        return """
            create table cars(id integer primary key, color text not null, gas_level double default 1.0);
            create table doors(carid integer, type text, primary key(carid, type));
            create table car_drivers(carid integer, driverid integer);
            create table drivers(id integer primary key, name text);
            create table blobs(oid blob primary key, data blob not null, num double, boo boolean);
            create table whatever(any primary key);
        """


gen_objs = MyOmen.codegen(out_path=temp_path())


class Car(gen_objs.cars_row):
    # every table has a row_type, you can derive from it
    def __init__(self, color="black", **kws):
        self.not_saved_to_db = "some thing"
        self.doors = gen_objs.doors_relation(
            self, kws.pop("doors", None), where={"carid": lambda: self.id}, cascade=True
        )
        self.car_drivers = gen_objs.car_drivers_relation(
            self, where={"carid": lambda: self.id}, cascade=True
        )
        super().__init__(color=color, **kws)

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


class Cars(gen_objs.cars[Car]):
    # redefine the row_type used
    row_type = Car


class InlineBasic(ObjBase):
    _pk = ("id",)
    id: int
    data: int

    # noinspection PyShadowingBuiltins
    def __init__(self, *, id=None, data=None, **kws):
        self.id = id
        self.data = data
        super().__init__(**kws)


class CarDriver(gen_objs.car_drivers_row):
    def __init__(self, **kws):
        self.drivers = gen_objs.drivers_relation(
            self, where={"id": lambda: self.driverid}, cascade=False
        )
        super().__init__(**kws)


class CarDrivers(gen_objs.car_drivers[CarDriver]):
    row_type = CarDriver
