import base64
import gc
import logging as log
from contextlib import suppress
from multiprocessing.pool import ThreadPool

import pytest
from notanorm import SqliteDb
from notanorm.errors import IntegrityError

from omen2 import Omen, ObjBase
from omen2.table import ObjCache, Table
from omen2.errors import OmenNoPkError, OmenKeyError, OmenMoreThanOneError
from tests.schema import MyOmen

# by calling force, code will always be regenerated... otherwise it's only regenerated if the import fails
MyOmen.codegen(force=True)

import tests.schema_gen as gen_objs

# every table has a row_type, you can derive from it


class CarDriver(gen_objs.car_drivers_row):
    def __init__(self, **kws):
        self.drivers = gen_objs.drivers_relation(
            self, where={"id": lambda: self.driverid}, cascade=False
        )
        super().__init__(**kws)


class CarDrivers(gen_objs.car_drivers[CarDriver]):
    row_type = CarDriver


class Car(gen_objs.cars_row):
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


# every db table has a type, you can derive from it
class Cars(gen_objs.cars[Car]):
    # redefine the row_type used
    row_type = Car


def test_readme(tmp_path):
    fname = str(tmp_path / "test.txt")
    db = SqliteDb(fname)

    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)

    # by default, you can always iterate on tables
    assert mgr.cars.count() == 0

    car = Car()  # creates a default car (black, full tank)
    car.color = "red"
    car.gas_level = 0.5
    car.doors.add(gen_objs.doors_row(type="a"))
    car.doors.add(gen_objs.doors_row(type="b"))
    car.doors.add(gen_objs.doors_row(type="c"))
    car.doors.add(gen_objs.doors_row(type="d"))
    mgr.cars.add(car)

    # cars have ids, generated by the db
    assert car.id

    with pytest.raises(AttributeError, match=r".*gas.*"):
        mgr.cars.add(Car(color="red", gas=0.3))

    mgr.cars.add(
        Car(
            color="red",
            gas_level=0.3,
            doors=[gen_objs.doors_row(type=str(i)) for i in range(4)],
        )
    )

    assert sum(1 for _ in mgr.cars.select(color="red")) == 2  # 2

    log.info("cars: %s", list(mgr.cars.select(color="red")))

    car = mgr.cars.select_one(color="red", gas_level=0.3)

    assert not car._meta.new

    with car:
        with pytest.raises(AttributeError, match=r".*gas.*"):
            car.gas = 0.9
        car.gas_level = 0.9
        types = set()
        for door in car.doors:
            types.add(int(door.type))
        assert types == set(range(4))

    # doors are inserted
    assert len(car.doors) == 4


# noinspection PyUnresolvedReferences
def test_weak_cache(caplog):
    # important to disable log capturing/reporting otherwise refs to car() could be out there!
    caplog.clear()
    caplog.set_level("ERROR")

    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = mgr.cars.add(Car(gas_level=0))

    # cache works
    car2 = mgr.cars.select_one(gas_level=0)
    assert id(car2) == id(car)
    assert len(mgr.cars._cache.data) == 1

    # weak dict cleans up
    del car
    del car2

    gc.collect()

    assert not mgr.cars._cache.data


def test_threaded():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = mgr.cars.add(Car(gas_level=0))
    pool = ThreadPool(10)

    def update_stuff(_i):
        with car:
            car.gas_level += 1

    # lots of threads can update stuff
    num_t = 10
    pool.map(update_stuff, range(num_t))
    assert car.gas_level == num_t

    # written to db
    assert mgr.db.select_one("cars", id=car.id).gas_level == num_t


def test_rollback():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(gas_level=2))

    with suppress(ValueError):
        with car:
            car.gas_level = 3
            raise ValueError

    assert car.gas_level == 2


def test_nopk():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    with pytest.raises(OmenNoPkError):
        mgr[gen_objs.doors].add(gen_objs.doors_row(carid=None, type=None))


def test_nodup():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    car = mgr[Cars].add(Car(gas_level=2))
    with pytest.raises(IntegrityError):
        mgr[Cars].add(Car(id=car.id, gas_level=3))


def test_shortcut_syntax():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    car = mgr[Cars].new(gas_level=2)
    assert car
    assert db.select_one("cars")
    assert mgr[Cars].get(car.id)
    assert not mgr[Cars].get("not a car id")
    assert car.id in mgr[Cars]
    assert car in mgr[Cars]
    # todo: why does __call__ not type hint properly?
    # noinspection PyTypeChecker
    assert mgr[Cars](car.id)
    with pytest.raises(OmenKeyError):
        # noinspection PyTypeChecker
        assert mgr[Cars]("not a car id")


def test_cache():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = Cars(mgr)
    orig = mgr.cars
    cars = ObjCache(mgr.cars)

    # replace the table with a cache
    mgr.cars = cars
    mgr.cars.add(Car(gas_level=2, color="green"))
    mgr.cars.add(Car(gas_level=3, color="green"))

    assert cars.select_one(id=1)

    assert cars.count() == 2

    assert orig._cache

    mgr.db.insert("cars", id=99, gas_level=99, color="green")
    mgr.db.insert("cars", id=98, gas_level=98, color="green")

    # this won't hit the db
    # noinspection PyUnresolvedReferences
    assert not any(car.id == 99 for car in mgr.cars.select())

    # this won't hit the db
    assert not cars.select_one(id=99)

    # this will
    assert cars.table.select(id=99)

    # now the cache is full
    assert cars.select(id=99)

    # but still missing others
    assert not cars.select_one(id=98)

    assert cars.reload() == 4

    log.debug(orig._cache)

    # until now
    assert cars.select_one(id=98)


def test_iter_and_sort():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    mgr.cars.add(Car(gas_level=2, color="green"))
    car = mgr.cars.add(Car(gas_level=3, color="green"))
    with car:
        car.doors.add(gen_objs.doors_row(type="z"))
        car.doors.add(gen_objs.doors_row(type="x"))

    # tables, caches and relations are all sortable, and iterable
    for door in car.doors:
        assert door
    sorted(car.doors)
    sorted(mgr.cars)

    cars = ObjCache(mgr.cars)
    mgr.cars = cars
    sorted(mgr.cars)


def test_cascade_relations():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(id=1, gas_level=2, color="green"))
    assert not car._meta.new

    with car:
        car.doors.add(gen_objs.doors_row(type="z"))
        car.doors.add(gen_objs.doors_row(type="x"))

    assert len(car.doors) == 2

    with car:
        car.id = 3

    assert len(car.doors) == 2
    assert mgr.cars.select_one(id=3)
    assert not mgr.cars.select_one(id=1)

    # cascaded remove
    assert len(list(mgr.db.select("doors"))) == 2

    car._remove()

    assert len(list(mgr.db.select("doors"))) == 0


def test_remove_driver():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    mgr.car_drivers = CarDrivers(mgr)
    Driver = gen_objs.drivers
    mgr.drivers = mgr[Driver]

    car1 = mgr.cars.add(Car(id=1, gas_level=2, color="green"))
    car2 = mgr.cars.add(Car(id=2, gas_level=2, color="green"))

    driver1 = mgr.drivers.new(name="bob")
    driver2 = mgr.drivers.new(name="joe")

    car1.car_drivers.add(CarDriver(carid=car1.id, driverid=driver1.id))
    car2.car_drivers.add(CarDriver(carid=car2.id, driverid=driver1.id))

    assert len(car1.car_drivers) == 1
    assert len(car2.car_drivers) == 1

    for cd in car1.car_drivers:
        car1.car_drivers.remove(cd)

    assert len(car1.car_drivers) == 0
    assert len(car2.car_drivers) == 1

    assert len(list(mgr.db.select("drivers"))) == 2
    assert len(list(mgr.db.select("car_drivers"))) == 1

    car1.car_drivers.add(CarDriver(carid=car1.id, driverid=driver1.id))
    assert len(car1.car_drivers) == 1
    mgr.car_drivers.remove(carid=car1.id, driverid=driver1.id)
    # ok to double-remove
    mgr.car_drivers.remove(carid=car1.id, driverid=driver1.id)
    # removing None is a no-op (ie: remove.(select_one(criteria....)))
    mgr.car_drivers.remove(None)
    assert len(car1.car_drivers) == 0
    assert len(car2.car_drivers) == 1

    car2.car_drivers.add(CarDriver(carid=car2.id, driverid=driver2.id))
    assert len(list(mgr.db.select("car_drivers"))) == 2
    with pytest.raises(OmenMoreThanOneError):
        # kwargs remove is not a generic method for removing all matching things
        # make your own loop if that's what you want
        mgr.car_drivers.remove(carid=car2.id)


def test_race_sync(tmp_path):
    fname = str(tmp_path / "test.txt")
    db = SqliteDb(fname)
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    ids = []

    def insert(i):
        h = mgr.cars.row_type(gas_level=i)
        mgr.cars.add(h)
        ids.append(h.id)

    num = 10
    pool = ThreadPool(10)

    pool.map(insert, range(num))

    assert mgr.cars.count() == num


def test_other_types():
    blob = gen_objs.blobs_row
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.blobs = mgr[gen_objs.blobs]
    mgr.blobs.add(blob(oid=b"1234", data=b"1234", num=2.4, boo=True))

    # cache works
    blob = mgr.blobs.select_one(oid=b"1234")
    assert blob.data == b"1234"
    assert blob.num == 2.4
    assert blob.boo
    with blob:
        blob.data = b"2345"
        blob.boo = False

    blob = mgr.blobs.select_one(oid=b"1234")
    assert blob.data == b"2345"
    assert not blob.boo


def test_any_type():
    whatever = gen_objs.whatever
    whatever_row = gen_objs.whatever_row
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr[whatever].add(whatever_row(any=31))
    mgr[whatever].add(whatever_row(any="str"))

    # cache works
    assert mgr[whatever].select_one(any=31)
    assert mgr[whatever].select_one(any="str")

    # mismatched types don't work
    assert not mgr[whatever].select_one(any="31")
    assert not mgr[whatever].select_one(any=b"str")


class InlineBasic(ObjBase):
    _pk = ("id",)

    # noinspection PyShadowingBuiltins
    def __init__(self, *, id=None, data=None, **kws):
        self.id = id
        self.data = data
        super().__init__(**kws)


def test_inline_omen_no_codegen():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data integer)"

    db = SqliteDb(":memory:")

    class Basic(InlineBasic):
        def _to_db(self):
            # modify data to the db
            return {"id": self.id, "data": self.data + 1}

        @classmethod
        def _from_db(cls, dct):
            dct = dct.copy()
            dct["data"] -= 1
            return super()._from_db(dct)

    class Basics(Table):
        table_name = "basic"
        row_type = Basic

    mgr = Harbinger(db)
    mgr.set_table(Basics(mgr))

    # simple database self-test

    data_set = {"basic": [{"id": 1, "data": 3}]}

    assert list(data_set.keys()) == list(mgr.table_types)
    mgr.load_dict(data_set)

    # backend stores transformed data
    assert mgr[Basics].select_one(id=1).data == 2

    dumped = mgr.dump_dict()
    assert dumped == data_set


def test_custom_data_type():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data text)"

    db = SqliteDb(":memory:")

    class Custom:
        def __init__(self, a, b):
            self.a = a
            self.b = b

        def _to_db(self):
            return self.a + "," + self.b

    class Basic(InlineBasic):
        @classmethod
        def _from_db(cls, dct):
            dct["data"] = Custom(*dct["data"].split(","))
            return Basic(**dct)

    # tests bootstrapping
    class Basics(Table[Basic]):
        pass

    mgr = Harbinger(db, basic=Basics)
    mgr.basic = Basics(mgr)
    mgr.basic.new(id=1, data=Custom("a", "b"))
    bas = mgr.basic.select_one(id=1)
    assert bas.data.a == "a"
    assert bas.data.b == "b"
    with bas:
        bas.data.a = "z"
    bas = mgr.basic.select_one(id=1)
    assert bas.data.a == "z"


def test_override_for_keywords():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table ents (id blob primary key, while, for, blob text)"

    db = SqliteDb(":memory:")

    class Ent(ObjBase):
        _pk = ("id",)

        # noinspection PyShadowingBuiltins
        def __init__(self, *, id=None, while_=None, for_=None, blob=None, **kws):
            self.id = id
            self.while_ = while_
            self.for_ = for_
            self.blob = blob
            super().__init__(**kws)

        def _to_db(self):
            return {
                "id": self.id,
                "while": self.while_,
                "for": self.for_,
                "blob": base64.b64encode(self.blob),
            }

        @classmethod
        def _from_db(cls, dct):
            kws = {
                "id": dct["id"],
                "while_": dct["while"],
                "for_": dct["for"],
                "blob": base64.b64decode(dct["blob"]),
            }
            return cls(**kws)

    class Ents(Table):
        row_type = Ent

    mgr = Harbinger(db, ents=Ents)

    # simple database self-test

    data_set = {
        "ents": [
            {"id": b"1234", "while": 1, "for": 3, "blob": base64.b64encode(b"1234")}
        ]
    }

    assert list(data_set.keys()) == list(mgr.table_types)
    mgr.load_dict(data_set)
    dumped = mgr.dump_dict()
    assert dumped == data_set
