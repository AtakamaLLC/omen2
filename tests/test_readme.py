import gc
import logging as log
from multiprocessing.pool import ThreadPool

import pytest
from notanorm import SqliteDb

from tests.schema import MyOmen

MyOmen.codegen()

import tests.schema_gen as gen_objs

# every table has a row_type, you can derive from it
class Car(gen_objs.cars_row):
    def __init__(self, color="black", **kws):
        self.not_saved_to_db = "some thing"
        self.doors = gen_objs.doors_relation(
            self, kws.pop("doors", None), carid=lambda: self.id
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
class Cars(gen_objs.cars):
    # redefine the row_type used
    row_type = Car


def test_readme(tmp_path):
    fname = str(tmp_path / "test.txt")
    db = SqliteDb(fname)

    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)

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

    with car:
        with pytest.raises(AttributeError, match=r".*gas.*"):
            car.gas = 0.9
        car.gas_level = 0.9

    # doors are inserted
    assert len(car.doors) == 4

    # cache works
    car2 = mgr.cars.select_one(color="red", gas_level=0.9)
    assert id(car2) == id(car)
    assert mgr.cars._Table__cache.data

    # weak dict cleans up
    del car
    del car2
    gc.collect()
    assert not mgr.cars._Table__cache.data


def test_threaded():
    db = SqliteDb(":memory:")
    # upon connection to a database, this will do migration, or creation as needed
    mgr = MyOmen(db, cars=Cars)
    car = mgr.cars.add(Car(gas_level=0))
    pool = ThreadPool(10)

    def update_stuff(i):
        with car:
            car.gas_level += 1

    # lots of threads can update stuff
    num_t = 10
    pool.map(update_stuff, range(num_t))
    assert car.gas_level == num_t

    # written to db
    assert mgr.db.select_one("cars", id=car.id).gas_level == num_t
