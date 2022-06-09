import sqlalchemy as sa

from mfm_learner import utils.utils


def is_table_exist(engine, name):
    return sa.inspect(engine).has_table(name)


def is_table_index_exist(engine, name):
    if not is_table_exist(engine, name):
        return False

    indices = sa.inspect(engine).get_indexes(name)
    return indices and len(indices) > 0


# python -m test.toy.test_db
if __name__ == '__main__':
    engine = utils.utils.connect_db()
    print(is_table_exist(engine, "test123"))
    print(is_table_index_exist(engine, "daily_basic"))
