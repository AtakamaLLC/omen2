import base64

import gc
import logging as log
import time
from contextlib import suppress
from multiprocessing.pool import ThreadPool
from unittest.mock import patch
from types import ModuleType

import pytest
from notanorm import SqliteDb
from notanorm.errors import IntegrityError

from omen2 import Omen, ObjBase, Relation, ObjCache
from omen2.object import CustomType
from omen2.table import Table
from omen2.errors import (
    OmenNoPkError,
    OmenKeyError,
    OmenMoreThanOneError,
    OmenUseWithError,
    OmenRollbackError,
    OmenLockingError,
)

from tests.schema import MyOmen, Cars, Car, InlineBasic, CarDriver, CarDrivers

import tests.schema_gen as gen_objs

# every table has a row_type, you can derive from it


def test_readme(tmp_path):
    fname = str(tmp_path / "test.txt")
    db = SqliteDb(fname)

    mgr = MyOmen(db)

    # cars pk is autoincrement
    assert Cars.allow_auto
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

    assert not car.id

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

    assert not car._is_new

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
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(gas_level=2))

    with suppress(ValueError):
        with car:
            car.gas_level = 3
            raise ValueError

    assert car.gas_level == 2

    with car:
        car.gas_level = 3
        raise OmenRollbackError

    assert car.gas_level == 2


def test_bigtx_rollback():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))
    car2 = mgr.cars.add(Car(gas_level=2))

    with mgr.transaction():
        car1.gas_level = 9
        car2.gas_level = 9
        raise OmenRollbackError

    assert car1.gas_level == 1
    assert car2.gas_level == 2


def test_bigtx_commit():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))
    car2 = mgr.cars.add(Car(gas_level=2))

    with mgr.transaction():
        car1.gas_level = 9
        car2.gas_level = 9

    assert car1.gas_level == 9
    assert car2.gas_level == 9


def test_tabtx():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))
    car2 = mgr.cars.add(Car(gas_level=2))

    with mgr.cars.transaction():
        car1.gas_level = 9
        car2.gas_level = 9

    assert car1.gas_level == 9
    assert car2.gas_level == 9

    with mgr.cars.transaction():
        car1.gas_level = 1
        car2.gas_level = 2
        raise OmenRollbackError

    assert car1.gas_level == 9
    assert car2.gas_level == 9


def test_bigtx_add_commit():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))

    with mgr.transaction():
        car1.gas_level = 9
        car2 = mgr.cars.add(Car(gas_level=2))

    assert car1.gas_level == 9
    assert car2.gas_level == 2

    assert len(mgr.cars) == 2


def test_bigtx_add_rollback():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))

    with mgr.transaction():
        car1.gas_level = 9
        car2 = mgr.cars.add(Car(gas_level=2))
        c2 = mgr.cars.select_one(gas_level=2)
        assert c2 is car2
        raise OmenRollbackError

    assert car1.gas_level == 1
    assert len(mgr.cars) == 1
    assert car1._is_bound

    # rollback unbinds object
    assert not car2._is_bound


def test_bigtx_remove_commit():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))
    car2 = mgr.cars.add(Car(gas_level=2))

    with mgr.transaction():
        mgr.cars.remove(car2)

    assert car1.gas_level == 1
    assert len(mgr.cars) == 1


def test_bigtx_remove_rollback():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(gas_level=1))
    car2 = mgr.cars.add(Car(gas_level=2))

    with mgr.transaction():
        mgr.cars.remove(car2)
        assert mgr.cars.select_one(gas_level=2) is None
        raise OmenRollbackError

    assert car1.gas_level == 1
    assert car2.gas_level == 2
    assert len(mgr.cars) == 2
    assert mgr.cars.select_one(gas_level=2) is car2


def test_bigtx_add_dup():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car1 = mgr.cars.add(Car(id=1, gas_level=1))

    with pytest.raises(IntegrityError):
        with mgr.transaction():
            car1.gas_level = 9
            mgr.cars.add(Car(id=2, gas_level=2))

            # this raises inline
            with pytest.raises(IntegrityError):
                mgr.cars.add(Car(id=2, gas_level=2))

            # this will raise on the way out
            car2 = mgr.cars.add(Car(id=3, gas_level=2))
            car2.id = 2
    assert len(mgr.cars) == 1
    assert mgr.cars.select_one(gas_level=1)


@patch("omen2.object.VERY_LARGE_LOCK_TIMEOUT", 0.1)
def test_deadlock():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(gas_level=2))

    def insert(i):
        try:
            with car:
                car.gas_level = i
                if i == 0:
                    time.sleep(1)
                return True
        except OmenLockingError:
            return False

    num = 3
    pool = ThreadPool(num)

    ret = pool.map(insert, range(num))

    assert sorted(ret) == [False, False, True]

    pool.terminate()
    pool.join()


def test_update_only():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(id=4, gas_level=2))
    with car:
        car.gas_level = 3
        # hack in the color....
        car.__dict__["color"] = "blue"
        assert "gas_level" in car._changes
        assert "color" not in car._changes
    # color doesn't change in db, because we only update "normally-changed" attributes
    assert db.select_one("cars", id=4).gas_level == 3
    assert db.select_one("cars", id=4).color == "black"


def test_nested_with():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(id=4, gas_level=2))
    with car:
        car.gas_level = 3
        with pytest.raises(OmenLockingError):
            with car:
                car.color = "blx"
    assert db.select_one("cars", id=4).gas_level == 3


def test_nopk():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    with pytest.raises(OmenNoPkError):
        mgr[gen_objs.doors].add(gen_objs.doors_row(carid=None, type=None))


def test_nodup():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    car = mgr[Cars].add(Car(gas_level=2))
    with pytest.raises(IntegrityError):
        mgr[Cars].add(Car(id=car.id, gas_level=3))


def test_need_with():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(gas_level=2))
    with pytest.raises(OmenUseWithError):
        car.doors = "green"
        assert car._manager is mgr
    assert mgr.cars.manager is mgr


def test_shortcut_syntax():
    db = SqliteDb(":memory:")
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
    mgr = MyOmen(db, cars=Cars)
    mgr.cars = mgr[Cars]
    car = mgr.cars.add(Car(id=1, gas_level=2, color="green"))
    assert not car._is_new

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

    w = mgr[whatever].select_one(any=31)
    with w:
        w.any = "change"
    assert mgr[whatever].select_one(any="change")


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


def test_inline_omen_from_module():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data integer)"

    db = SqliteDb(":memory:")

    class Basic(InlineBasic):
        pass

    class Basics(Table):
        table_name = "basic"
        row_type = Basic

    module = ModuleType("<inline>")
    module.Basics = Basics
    mgr = Harbinger(db, module)
    mgr[Basics].add(Basic(id=1))


def test_custom_data_type():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data text)"

    db = SqliteDb(":memory:")

    class Custom(CustomType):
        # by deriving from CustomType, we track-changes properly
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
        bas.data = Custom("z", "b")
    bas = mgr.basic.select_one(id=1)
    assert bas.data.a == "z"

    # if you don't derive from CustomType, then this will not work
    with bas:
        # partial change to object is tracked as change to main data type
        bas.data.a = "x"
    bas = mgr.basic.select_one(id=1)
    assert bas.data.a == "x"


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


def test_unbound_basics():
    c1 = Car(id=4, color="green", gas_level=5)
    c2 = Car(id=5, color="green", gas_level=5)
    s = {c1, c2}
    assert c1 in s
    assert c2 in s
    assert c1 != c2
    cx = Car(id=4, color="green", gas_level=5)

    # equality is defined by primary key equality
    assert cx == c1

    dct = c1._to_db()
    assert str(c1) == str(dct)

    cx.id = None

    with pytest.raises(OmenNoPkError):
        # no pk, cannot compare
        assert not cx == c1

    # ok to use with on unbound - does nothing
    with c1:
        pass


def test_threaded_reads():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = mgr.cars.add(Car(gas_level=0, color=str(0)))
    pool = ThreadPool(50)

    # to reproduce the problem with this, you need to catch python while switching contexts
    # in between setattr calls in a non-atomic "apply" function
    # this is basically impossible without sticking a time sleep in there
    # even with 100 attributes and 5000 threads it never failed
    # so this test case only tests if the atomic apply is totally/deeply broken

    def update_stuff(_i):
        time.sleep(0.00001)
        assert car.color == str(car.gas_level)
        with car:
            car.gas_level += 1
            time.sleep(0.00001)
            car.color = str(car.gas_level)
        assert car.color == str(car.gas_level)
        time.sleep(0.00001)
        assert car.color == str(car.gas_level)

    # lots of threads can update stuff
    num_t = 10
    pool.map(update_stuff, range(num_t))
    assert car.gas_level == num_t


def test_thread_locked_writer_only():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = mgr.cars.add(Car(gas_level=0, color=str(0)))
    num_t = 15
    num_w = 3
    pool = ThreadPool(num_t)

    # to reproduce the problem with this, you need to catch python while switching contexts
    # in between setattr calls in a non-atomic "apply" function
    # this is basically impossible without sticking a time sleep in there
    # even with 100 attributes and 5000 threads it never failed
    # so this test case only tests if the atomic apply is totally/deeply broken

    def update_stuff(i):
        if i < num_w:
            with car:
                car.gas_level += 1
                car.color = str(car.gas_level)
                time.sleep(0.1)
        else:
            with pytest.raises(OmenUseWithError):
                car.gas_level += 1

    # lots of threads can update stuff
    pool.map(update_stuff, range(num_t))
    assert car.gas_level == num_w


def test_setter_getter():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data text)"

    db = SqliteDb(":memory:")

    class Basic(InlineBasic):
        __data = None

        @property
        def data(self):
            return self.__data

        @data.setter
        def data(self, val):
            self.__data = str(val)

    # tests bootstrapping
    class Basics(Table[Basic]):
        pass

    mgr = Harbinger(db, basic=Basics)
    mgr.basic = Basics(mgr)
    mgr.basic.new(id=1, data="someval")
    bas = mgr.basic.select_one(id=1)
    assert bas.data == "someval"

    # bypass normal checks
    bas._Basic__data = "otherval"

    # reread-from db overrides/fixes cached objects atomically
    mgr.basic.select_one(id=1)
    assert bas.data == "someval"


def test_reload_from_disk():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = mgr.cars.add(Car(gas_level=0, color="green"))
    assert db.select_one("cars", id=1).color == "green"
    db.update("cars", id=1, color="blue")
    # doesn't notice db change because we have a weakref-cache
    assert car.color == "green"
    car2 = mgr.cars.select_one(id=1)
    # weakref cache
    assert car2 is car
    # db fills to cache on select
    assert car.color == "blue"


def test_other_attrs():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data text)"

    db = SqliteDb(":memory:")

    class Basic(InlineBasic):
        def __init__(self, *, id, data, **kws):
            self.custom_thing = 44
            super().__init__(id=id, data=data, **kws)

    # tests bootstrapping
    class Basics(Table[Basic]):
        pass

    mgr = Harbinger(db, basic=Basics)
    mgr.basic = Basics(mgr)
    mgr.basic.new(id=1, data="someval")
    bas = mgr.basic.select_one(id=1)
    assert bas.custom_thing == 44
    with pytest.raises(OmenUseWithError):
        bas.custom_thing = 3

    assert mgr.basic.select_one(custom_thing=44)
    assert not mgr.basic.select_one(custom_thing=43)


def test_disable_allow_auto():
    db = SqliteDb(":memory:")
    with patch.object(Cars, "allow_auto", False):
        mgr = MyOmen(db)
        mgr.cars = Cars(mgr)
        with pytest.raises(OmenNoPkError):
            mgr.cars.add(Car(gas_level=0, color="green"))


def test_multi_pk_issues():
    # noinspection PyAbstractClass
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return (
                "create table basic (id1 integer, id2 integer, primary key (id1, id2));"
                "create table subs (id1 integer, id2 integer, sub text)"
            )

    class Basic(ObjBase):
        _pk = ("id1", "id2")

        def __init__(self, *, id1=None, id2=None, **kws):
            self.id1 = id1
            self.id2 = id2
            self.subs = SubsRel(self, where={"id1": id1, "id2": id2}, cascade=True)
            super().__init__(**kws)

    class Sub(ObjBase):
        _pk = ("id1", "id2")

        def __init__(self, *, id1=None, id2=None, sub, **kws):
            self.id1 = id1
            self.id2 = id2
            self.sub = sub
            super().__init__(**kws)

    db = SqliteDb(":memory:")

    class Basics(Table):
        table_name = "basic"
        row_type = Basic

    class Subs(Table):
        table_name = "subs"
        row_type = Sub

    class SubsRel(Relation[Sub]):
        table_type = Subs

    mgr = Harbinger(db)
    mgr.set_table(Basics(mgr))
    mgr.set_table(Subs(mgr))
    basics = mgr[Basics]

    with pytest.raises(OmenNoPkError):
        basics.new(id1=4)

    b = basics.new(id1=4, id2=5)
    b.subs.add(Sub(sub="what"))

    assert db.select_one("subs", id1=4, id2=5).sub == "what"


def test_unbound_add():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = Car(id=44, gas_level=0, color="green")
    door = gen_objs.doors_row(type="a")
    car.doors.add(door)
    assert door.carid == car.id
    assert door.carid
    assert door in car.doors
    assert car.doors.select_one(type="a")
    mgr.cars.add(car)
    assert db.select_one("doors", carid=car.id, type="a")


def test_underlying_delete():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = Car(id=44, gas_level=0, color="green")
    mgr.cars.add(car)
    assert db.select_one("cars", id=car.id)
    db.delete("cars", id=car.id)
    car2 = Car(id=44, gas_level=0, color="green")
    mgr.cars.add(car2)
    assert car2 is not car
    assert mgr.cars.get(id=44) is car2


def test_sync_on_getattr():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    car = Car(id=44, gas_level=0, color="green")
    mgr.cars.add(car)
    car._sync_on_getattr = True
    assert car.color == "green"
    db.update("cars", id=car.id, color="blue")
    assert car.color == "blue"


def test_cache_sharing():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    cache = ObjCache(mgr.cars)
    car = Car(id=44, gas_level=0, color="green")
    cache.add(car)
    assert car in mgr.cars
    assert car in cache
    db.insert("cars", id=45, gas_level=45, color="blue")
    db.insert("cars", id=46, gas_level=46, color="red")
    assert cache.get(id=44)
    assert not cache.select_one(id=45)
    assert not cache.select_one(id=46)
    assert mgr.cars.select_one(id=45)
    assert cache.select_one(id=45)
    assert mgr.cars.select_one(id=46)
    assert cache.select_one(id=46)

    db.delete("cars", id=44)
    # still cached
    assert cache.get(id=44)
    assert cache.select_one(id=44)
    # still cached, select with a different filter does not clear cache
    assert mgr.cars.select_one(id=45)
    assert cache.select_one(id=44)
    # cache updated by select with matching filter
    assert not mgr.cars.select_one(id=44)
    assert not cache.select_one(id=44)
    # select() with no filter clears cache
    db.delete("cars", id=46)
    assert len(list(mgr.cars.select())) == 1
    assert not cache.select_one(id=46)
    # reload() clears cache
    assert cache.get(id=45)
    db.delete("cars", id=45)
    cache.reload()
    assert not cache.select_one(id=45)


def test_cache_sharing_threaded():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    mgr.cars = Cars(mgr)
    cache = ObjCache(mgr.cars)
    db.insert("cars", id=12, gas_level=0, color="green")
    assert mgr.cars.select_one(id=12)
    assert cache.select_one(id=12)

    # all threads update gas_level of cached car, only the first thread reloads the cache
    def update_stuff(_i):
        if _i == 0:
            cache.reload()

        # if the cache was cleared in in another thread, this returns None (behavior we want to avoid)
        c = cache.select_one(id=12)
        assert c
        with c:
            c.gas_level += 1

    num_t = 10
    pool = ThreadPool(num_t)
    pool.map(update_stuff, range(num_t))
    assert cache.select_one(id=12).gas_level == num_t
