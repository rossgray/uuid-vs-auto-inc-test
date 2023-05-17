import time
from sqlalchemy import create_engine, text

NUM_ITERATIONS = 10_000

AUTO_INC_TABLE_NAME = "test_auto_inc"
UUID_TABLE_NAME = "test_uuid"
UUID_STR_TABLE_NAME = "test_uuid_str"

CREATE_TABLE_AUTO_INC_SQL = f"""
CREATE TABLE IF NOT EXISTS {AUTO_INC_TABLE_NAME} (
    id SERIAL,
    position INT NOT NULL,
    PRIMARY KEY (id)
);
"""
CREATE_TABLE_UUID_SQL = f"""
CREATE TABLE IF NOT EXISTS {UUID_TABLE_NAME} (
    id UUID DEFAULT gen_random_uuid (),
    position INT NOT NULL,
    PRIMARY KEY (id)
);
"""
CREATE_TABLE_UUID_STR_SQL = f"""
CREATE TABLE IF NOT EXISTS {UUID_STR_TABLE_NAME} (
    id TEXT DEFAULT gen_random_uuid (),
    position INT NOT NULL,
    PRIMARY KEY (id)
);
"""


engine = create_engine(
    "postgresql+psycopg2://postgres:topsecret@localhost:5432/postgres",
    # echo=True
)


def create_tables(conn):
    conn.execute(text(CREATE_TABLE_AUTO_INC_SQL))
    conn.execute(text(CREATE_TABLE_UUID_SQL))
    conn.execute(text(CREATE_TABLE_UUID_STR_SQL))

    # ensure both tables are empty in case they have data from a previous run
    conn.execute(text(f"DELETE FROM {AUTO_INC_TABLE_NAME}"))
    conn.execute(text(f"DELETE FROM {UUID_TABLE_NAME}"))
    conn.execute(text(f"DELETE FROM {UUID_STR_TABLE_NAME}"))
    conn.commit()


def _insert_into_table(conn, table_name, num_iterations):
    start = time.perf_counter()
    for i in range(num_iterations):
        # insert a single row, and committing to simulate a single operation
        conn.execute(
            text(f"INSERT INTO {table_name} (position) VALUES (:pos)"), [{"pos": i}]
        )
        conn.commit()
    end = time.perf_counter()
    time_taken = end - start
    return time_taken


def insert_into_table(conn, table_name):
    """Insert half the rows into an empty table, then pause, and insert the remaining rows.

    This is done, just in case it makes a difference to insert into an alredy
    populated table vs an empty table (e.g. if it takes a while for the indexes
    to be built)
    """
    time_taken = _insert_into_table(conn, table_name, NUM_ITERATIONS // 2)
    time.sleep(1)
    time_taken += _insert_into_table(conn, table_name, NUM_ITERATIONS // 2)

    # check number of entries
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    assert result.fetchone()[0] == NUM_ITERATIONS
    print(f"Inserted {NUM_ITERATIONS} rows into {table_name} in {time_taken:.2f}s")
    return time_taken


def select_from_table(conn, table_name):
    # first, fetch all primary keys from the table
    result = conn.execute(text(f"SELECT id FROM {table_name}"))
    ids = [row.id for row in result]
    # convert uuids to strings (seem to perform better in queries)
    ids = [str(id) for id in ids] if not isinstance(ids[0], int) else ids

    start = time.perf_counter()
    for id in ids:
        result = conn.execute(
            text(f"SELECT * FROM {table_name} WHERE id = :id"), {"id": id}
        )
        assert result.fetchone()
    end = time.perf_counter()
    time_taken = end - start
    print(f"Selected {NUM_ITERATIONS} rows from {table_name} in {time_taken:.2f}s")
    return time_taken


def main():
    with engine.connect() as conn:
        create_tables(conn)
        time_insert_auto_inc = insert_into_table(conn, AUTO_INC_TABLE_NAME)
        time_insert_uuid = insert_into_table(conn, UUID_TABLE_NAME)
        time_insert_uuid_str = insert_into_table(conn, UUID_STR_TABLE_NAME)
        time_select_auto_inc = select_from_table(conn, AUTO_INC_TABLE_NAME)
        time_select_uuid = select_from_table(conn, UUID_TABLE_NAME)
        time_select_uuid_str = select_from_table(conn, UUID_STR_TABLE_NAME)


if __name__ == "__main__":
    main()
