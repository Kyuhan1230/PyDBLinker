import os
import re
import sqlite3


class SQLiteConnection:
    """Context manager for SQLite connection.

    Usage:
        with SQLiteConnection(db_name) as conn:
            # use the connection
            ...
    """
    def __init__(self, db_name: str):
        self.db_name = db_name
        if not os.path.exists(db_name):
            raise FileNotFoundError(f"The database file {db_name} does not exist.")

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        self.conn.close()

def query_db(db_name, query, args=(), many=False, one=False):
    """Execute a query on the database.

    Args:
        db_name (str): The name(file path) of db to connect
        query (str): The SQL query to execute.
        args (tuple, optional): The values to substitute into the query.
        many (bool, optional): Whether to execute "executemany" instead of "execute".
        one (bool, optional): Whether to return only one row from a SELECT query.

    Returns:
        For SELECT queries: a list of rows or a single row (if one=True).
        For other queries: the number of rows affected.

    Examples:
        # 사용 예시
        try:
            result = query_db("SELECT * FROM some_table")
        except Exception as e:
            print(f"An error occurred: {e}")
    """
    if not query:
        raise ValueError("The query string cannot be empty.")
    
    with SQLiteConnection(db_name) as conn:
        cur = conn.cursor()
        try:
            if many:
                result = cur.executemany(query, args)
            else:
                result = cur.execute(query, args)

            # Check if the query is a SELECT query
            if re.match(r"(SELECT)", query, re.I):
                rv = result.fetchall()
                return (rv[0] if rv else None) if one else rv
            else:
                return conn.total_changes  # 반환 값 변경: 영향 받은 행의 수
        except Exception as E:
            print(f"Query Error: {db_name}, {query}, \n{E}, \n{args}")
            raise

def batch_delete_tables(db_name, table_name, batch_size=1000):
    """Deletes rows from the specified table in batches until all rows are deleted.

    Args:
        db_name (str): The name(file path) of db to connect.
        table_name (str): The name of the table to delete rows from.
        batch_size (int, optional): The number of rows to delete in each batch.

    Usage:
        batch_delete_tables('my_database.db', 'my_table', 500)
    """
    # 데이터를 삭제할 때 사용할 기본 쿼리
    delete_query = f"DELETE FROM {table_name} LIMIT {batch_size}"

    # 삭제된 행의 총 개수
    total_deleted = 0

    while True:
        # query_db 함수를 이용해 데이터 삭제 실행
        deleted_rows = query_db(db_name, delete_query)
        
        # 삭제된 행이 없으면 반복 종료
        if deleted_rows == 0:
            break
        
        # 삭제된 행의 수를 누적
        total_deleted += deleted_rows

        print(f"{deleted_rows} rows deleted from {table_name}. Total deleted: {total_deleted}")

def connection_test(db_name):
    """Test the connection to the SQLite database."""
    try:
        with SQLiteConnection(db_name) as conn:
            print("Successfully connected to SQLite database.")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
