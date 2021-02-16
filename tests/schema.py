from omen2 import Omen


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
        """

    @classmethod
    def migrate(cls, db, version):
        # you should create a migration for each version
        assert False