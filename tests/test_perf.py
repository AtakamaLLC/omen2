# SPDX-FileCopyrightText: © Atakama, Inc <support@atakama.com>
# SPDX-License-Identifier: LGPL-3.0-or-later

import importlib
import os
import tempfile
import time
import subprocess
import shutil
import sys
import logging as log
from contextlib import contextmanager

from notanorm import SqliteDb

SECS = 1
LOOPS = 100


def do_perf_check(init, func, secs=SECS, loop=LOOPS):
    args = init()
    start = time.time()
    end = start
    ctr = 0
    while (end - start) < secs:
        ctr += loop
        for _ in range(loop):
            func(*args)
        end = time.time()
    import omen2.object

    dur = end - start
    log.error("perf check %s: %s/%s = %s", omen2.object.__file__, dur, ctr, dur / ctr)
    return dur / ctr


@contextmanager
def get_omen2_tmpd(version):
    tmpd = os.path.join(tempfile.gettempdir(), "omen2-perf-test", "ver" + version)

    if not os.path.exists(tmpd):
        try:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--target",
                    tmpd,
                    "omen2==" + version,
                ]
            )
        except Exception:
            shutil.rmtree(tmpd)
            raise

    yield tmpd


def clear_omen():
    import omen2

    cur1 = os.path.dirname(omen2.__file__)
    delete = []
    for name, mod in sys.modules.items():
        mod_path = getattr(mod, "__file__", "")
        if cur1 in mod_path:
            delete += [name]

    for ent in delete:
        del sys.modules[ent]

    for ent in delete:
        __import__(ent)


@contextmanager
def swap_version(version):
    with get_omen2_tmpd(version) as om:
        save = sys.path.copy()
        sys.path = [om]

        clear_omen()

        try:
            yield
        finally:
            sys.path = save
            clear_omen()


def perf_check(init, func, version=None, secs=SECS, loop=LOOPS):
    if version:
        with swap_version(version):
            return do_perf_check(init, func, secs=secs, loop=loop)
    else:
        return do_perf_check(init, func, secs=secs, loop=loop)


def test_ver_swap():
    import omen2

    f1 = omen2.__file__
    with swap_version("1.4.3"):
        import omen2

        assert omen2.__file__ != f1


def test_perf_attr():
    def init():
        import omen2
        import tests.schema

        importlib.reload(tests.schema)
        from tests.schema import MyOmen, Cars, Car

        db = SqliteDb(":memory:")
        mgr = MyOmen(db)
        mgr.cars = Cars(mgr)
        car = Car(id=44, gas_level=0, color="green")
        mgr.cars.add(car)
        import sys

        print(sys.modules[type(car).__module__].__file__)
        return (car,)

    def check(car):
        for _ in range(1000):
            _ = car.gas_level
            _ = car.id
            _ = car.color
            _ = car.gas_level
            _ = car.id
            _ = car.color

    t1 = perf_check(init, check)
    t2 = perf_check(init, check, version="1.4.3")

    log.error("before: %s, after: %s", t2, t1)

    assert t1 < t2
