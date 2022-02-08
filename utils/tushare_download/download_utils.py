import sqlalchemy


def is_table_exist(engine, name):
    return sqlalchemy.inspect(engine).has_table(name)


def is_table_index_exist(engine, name):
    if not is_table_exist(engine, name):
        return False

    indices = sqlalchemy.inspect(engine).get_indexes(name)
    return indices and len(indices) > 0
