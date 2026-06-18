
import sqlite3

def test_sqlite3_import():
    print(f"sqlite3: {sqlite3}")
    conn = sqlite3.connect(":memory:")
    print(f"conn: {conn}")
    assert conn is not None
    conn.close()

def test_sqlite3_operational_error():
    try:
        raise sqlite3.OperationalError("test")
    except sqlite3.OperationalError as e:
        assert str(e) == "test"
