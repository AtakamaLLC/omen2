from typing import List, Any, Optional, Union

import notanorm.errors
import pytest
from notanorm import SqliteDb

from omen2 import Omen
from omen2.table import Table
from tests.schema import MyOmen, Car, Cars, InlineBasic


def test_type_checking():
    db = SqliteDb(":memory:")
    mgr = MyOmen(db)
    Car._type_check = True

    mgr.cars = Cars(mgr)
    car = mgr.cars.add(Car(gas_level=0, color="green"))

    with pytest.raises(TypeError):
        with car:
            car.color = None

    with pytest.raises(TypeError):
        with car:
            car.color = 4

    with pytest.raises(TypeError):
        with car:
            car.gas_level = "hello"

    with pytest.raises(TypeError):
        with car:
            car.color = b"ggh"

    car._type_check = False
    # notanorm integrity error because color is a required field
    with pytest.raises(notanorm.errors.IntegrityError):
        with car:
            car.color = None

    # sqlite allows this, so we do too, since type checking is off
    with car:
        car.color = b"ggh"


def test_type_custom():
    class Harbinger(Omen):
        @classmethod
        def schema(cls, version):
            return "create table basic (id integer primary key, data integer)"

    class Basic(InlineBasic):
        _type_check = True
        other: Any
        flt: float
        wack: List[str]
        opt: Optional[str]
        un: Union[str, int]

        def __init__(
            self, id, data, other, flt, wack=None, opt=None, un: Union[str, int] = 0
        ):
            self.other = other
            self.flt = flt
            self.wack = wack or []
            self.opt = opt
            self.un = un
            super().__init__(id=id, data=data)

    class Basics(Table):
        table_name = "basic"
        row_type = Basic

    db = SqliteDb(":memory:")
    mgr = Harbinger(db)
    mgr.basics = Basics(mgr)

    Basic(4, 5, 6, 7)

    # list of integers is allowed, because we don't check complex types, this is mostly for sql checking!
    Basic(4, 5, 6, 7, [9])
    Basic(4, 5, 6, 7.1, [9])

    with pytest.raises(TypeError):
        Basic(4, 5, 6, "not")

    with pytest.raises(TypeError):
        Basic(4.1, 5, 6, 7)

    with pytest.raises(TypeError):
        Basic(4, 5, 6, 7, opt=5)

    Basic(4, 5, 6, 7, opt=None)

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        Basic(4, 5, 6, 7, un=None)
    Basic(4, 5, 6, 7, un=4)
    Basic(4, 5, 6, 7, un="whatever")
