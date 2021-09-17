import pytest
from notanorm import SqliteDb

from omen2 import Omen, OmenKeyError
from omen2.m2mhelper import M2MHelper


class Harbinger(Omen):
    @classmethod
    def schema(cls, version):
        return """
            create table groups (id integer primary key, data text);
            create table peeps (id integer primary key, data text);
            create table group_peeps (groupid integer, peepid integer, role text, primary key (groupid, peepid));
            """


Harbinger.codegen()
from . import test_m2mhelper_gen as module

# noinspection PyShadowingBuiltins
class Group(module.groups_row):
    def __init__(self, id=None, data=None):
        self.peeps = M2MHelper(
            self,
            types=(GroupPeeps, Peeps),
            where=({"groupid": "id"}, {"peepid": "id"}),
        )
        super().__init__(id=id, data=data)

    def __lt__(self, other: "Group"):
        return self.data < other.data

    def same_method(self):
        return "group"


# noinspection PyShadowingBuiltins
class Peep(module.peeps_row):
    def __init__(self, id=None, data=None):
        self.groups = M2MHelper(
            self,
            types=(GroupPeeps, Groups),
            where=({"peepid": "id"}, {"groupid": "id"}),
        )
        super().__init__(id=id, data=data)

    def __lt__(self, other: "Peep"):
        return self.data < other.data

    def same_method(self):
        return "peep"


class Groups(module.groups[Group]):
    row_type = Group


class Peeps(module.peeps[Peep]):
    row_type = Peep


class GroupPeep(module.group_peeps_row):
    def __lt__(self, other: "GroupPeep"):
        return self.role < other.role

    def same_method(self):
        return "group_peep"


class GroupPeeps(module.group_peeps):
    row_type = GroupPeep


def test_m2m_multi_inherit():
    db = SqliteDb(":memory:")
    mgr = Harbinger(db, groups=Groups, peeps=Peeps)
    mgr.groups = Groups(mgr)
    mgr.peeps = mgr[Peeps]
    grp1 = mgr.groups.new(id=1, data="g1")
    grp2 = mgr.groups.new(id=2, data="g2")
    peep1 = mgr.peeps.new(id=2, data="p1")
    peep2 = mgr.peeps.new(id=3, data="p2")
    res1 = grp1.peeps.add(peep1, role="role")
    res2 = grp2.peeps.add(peep2, role="role2")

    assert len(grp1.peeps) == 1
    assert len(grp2.peeps) == 1

    assert res1.role == "role"
    assert res1.data == "p1"

    assert res2.role == "role2"
    assert res2.data == "p2"

    peep1 = grp1.peeps.get(role="role")

    peep_by_id = grp1.peeps.get(id=2)
    assert peep_by_id == peep1

    assert not grp2.peeps.get(role="role")
    assert grp2.peeps.get(role="role2")

    for gr in peep1.groups:
        assert gr.role == "role"

    with peep1.groups(1) as gr:
        gr.role = "new_role"
        gr.data = "new_data"

    assert mgr.db.select_one("groups", id=1).data == "new_data"
    assert mgr.db.select_one("group_peeps", groupid=1).role == "new_role"

    grp1.peeps.remove(peep1)

    assert mgr.db.select_one("groups", id=1).data == "new_data"
    assert not mgr.db.select_one("group_peeps", groupid=1)

    # you can add by id too
    grp1.peeps.add(peep1.id, role="role3")
    assert db.select_one("group_peeps", groupid=1).role == "role3"

    # you can get by id too
    assert grp1.peeps(peep1.id).role == "role3"
    assert grp1.peeps(id=peep1.id).role == "role3"
    assert grp1.peeps(peepid=peep1.id).role == "role3"
    assert grp1.peeps.get(peep1.id).role == "role3"


def test_m2m_add():
    db = SqliteDb(":memory:")
    mgr = Harbinger(db, groups=Groups, peeps=Peeps)
    mgr.groups = Groups(mgr)
    mgr.peeps = mgr[Peeps]
    grp1 = mgr.groups.new(id=1, data="g1")
    peep1 = mgr.peeps.new(id=2, data="p1")
    mix1 = grp1.peeps.add(peep1.id, role="role")
    assert mix1.role == "role"
    assert db.select_one("group_peeps", groupid=1).peepid == 2
    with pytest.raises(OmenKeyError):
        grp1.peeps.add(99, role="role")
    grp1.peeps.remove(peep1.id)
    # gone
    assert db.select_one("group_peeps", groupid=1) is None

    # ok to remove more than once, silently ignored
    grp1.peeps.remove(peep1.id)
    grp1.peeps.remove(peep1.id)
    assert grp1.peeps.get(peep1.id) is None

    # add/remove obj
    grp1.peeps.add(peep1)
    assert grp1.peeps.get(peep1.id)
    assert peep1.id in grp1.peeps
    grp1.peeps.remove(peep1)
    assert grp1.peeps.get(peep1.id) is None


def test_m2m_subsorts():
    db = SqliteDb(":memory:")
    mgr = Harbinger(db, groups=Groups, peeps=Peeps, group_peeps=GroupPeeps)
    mgr.groups = Groups(mgr)
    mgr.peeps = mgr[Peeps]
    grp1 = mgr.groups.new(id=1, data="g1")
    peep1 = mgr.peeps.new(id=2, data="p1")
    peep2 = mgr.peeps.new(id=3, data="p2")
    peep3 = mgr.peeps.new(id=4, data="p3")
    peep4 = mgr.peeps.new(id=5, data="p0")

    # should sort by role, and then by peep
    mix4 = grp1.peeps.add(peep4, role="role3")
    mix2 = grp1.peeps.add(peep1, role="role2")
    mix3 = grp1.peeps.add(peep2, role="role2")
    mix1 = grp1.peeps.add(peep3, role="role1")

    # check that m2m table properties properly override other props
    assert peep1.groups(1).same_method() == "group_peep"
    assert mix1.same_method() == "group_peep"

    # add function sort
    all = [mix1, mix2, mix3, mix4]
    srt = sorted(all)
    expect = [mix1, mix2, mix3, mix4]

    assert srt == expect

    # select function sort
    srt = sorted(grp1.peeps)

    assert srt == expect


@pytest.mark.parametrize("who_adds", [Group, Peep])
def test_m2m_unbound(who_adds):
    db = SqliteDb(":memory:")
    mgr = Harbinger(db, groups=Groups, peeps=Peeps, group_peeps=GroupPeeps)
    mgr.groups = Groups(mgr)
    mgr.peeps = mgr[Peeps]
    grp1 = Group(id=1, data="g1")
    peep1 = Peep(id=2, data="p1")
    grp1.peeps.add(peep1, role="role")

    assert peep1 in grp1.peeps
    assert peep1.id in grp1.peeps

    if who_adds is Group:
        mgr.groups.add(grp1)
    else:
        mgr.peeps.add(peep1)

    assert peep1.id in mgr.peeps
    assert peep1.id in grp1.peeps
    assert peep1 in mgr.peeps
    assert peep1 in grp1.peeps